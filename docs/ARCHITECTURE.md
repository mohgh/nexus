# Nexus — Architecture Map

A navigation aid for reading the codebase. The [README](../README.md) says *what each
package answers*; this says *how a request moves through them, in what order, and which
files to read first*.

---

## 1. The mental model

Three things explain almost every design decision in this repo:

1. **The binary grows by chapter.** `cmd/server/main.go` is a single linear wiring
   function, annotated `Ch01 … Ch14`. Nothing is registered by `init()`, there is no DI
   container, no global state. Read it top to bottom and you have seen the whole system.

2. **Postgres is the only required dependency.** Redis, ClickHouse, Kafka, Temporal are
   each wired inside an `if err != nil { logger.Warn(...) }` block. When one is down the
   feature *disappears* — including its HTTP routes. This is why `Router()` is full of
   `if s.chaos != nil` guards: **the route table is a function of which services are up.**

3. **Capability is expressed as `nil` or not-`nil`.** `Server` is one struct of optional
   dependencies (`internal/api/router.go:36-123`), populated by `WithX()` builders. A nil
   field means "that chapter isn't running." One subtlety worth knowing, documented at
   `cmd/server/main.go:148-153`: passing a nil `*MemoryLimiter` through the `Limiter`
   *interface* produces a non-nil interface value and defeats the nil check — so the
   wiring guards it explicitly.

---

## 2. Trace one request end to end

`POST /api/v1/events` is the single best path to follow. It touches auth, rate limiting,
idempotency, the decorator stack, a two-table transaction, Kafka, and the async
projection loop.

### 2a. The middleware chain, in exact execution order

From `internal/api/router.go:347-449`. Order matters and the comments explain why.

| # | Middleware | Source | Note |
|---|---|---|---|
| 1 | `RequestID`, `RealIP`, `Recoverer` | chi | |
| 2 | `exposeRequestID` | `router.go:591` | copies request ID to the response header so structured errors can cite it |
| 3 | `CORS` | `middleware/cors.go` | no-op when no origins configured |
| 4 | `PIIDetect` | `middleware/pii.go` | **must precede metrics** — it stamps the masked path into ctx that the access log then reads |
| 5 | `metricsMiddleware` | `router.go:621` | labels use the **chi route pattern**, never `r.URL.Path` |
| 6 | `Timeout(10s)` | chi | inside the 15s `http.Server` write timeout |
| 7 | `RYOWMiddleware` | `handlers/` | reads `X-Nexus-Min-LSN`, stamps `X-Nexus-Write-LSN` on writes |
| — | *route group split* | | three auth tiers, below |
| 8 | `Authenticate` | `middleware/auth.go:42` | 401 on missing/unknown/revoked key |
| 9 | `RequireAdmin` **or** `EnforceTenantParam` | `auth.go:62` / `auth.go:77` | tier-dependent; absent on the plain tenant tier |
| 10 | `RateLimit` | `middleware/ratelimit.go` | **after** auth — buckets are keyed and sized by the authenticated principal's plan |
| 11 | `Idempotency` | `middleware/idempotency.go` | **after** auth — cached keys are namespaced by principal, so one tenant's key can't return another's cached body |

The ordering constraints in rows 4, 10 and 11 are the interesting part: each is a
correctness requirement, not a style choice.

### 2b. Two things worth pausing on

**Metric labels (`router.go:600-620`).** Labels carry the route pattern, not the URL, for
two stated reasons: a raw path can contain emails or IPs that would then persist in
Prometheus for the retention window, and one series per distinct URL explodes cardinality
on UUID path params. The access log takes the opposite trade — masked *actual* path,
because debugging needs the real URL minus the PII. The scrape endpoint excludes itself
so 15-second 200s don't skew the SLO latency percentiles.

**Idempotency is reserve-then-execute (`idempotency.go:37-70`).** It `INSERT`s an
`in_flight` row *before* running the handler; the PK conflict is the dedup boundary, so
concurrent retries see each other. Only 2xx responses are cached (a transient 500 stays
retryable), the full response is stored so a replay returns identical bytes, and bodies
over 1 MiB skip the cache rather than risk an OOM. The header comment names the race in
the earlier lookup-then-execute version — both retries could run a side-effecting
handler. Read this one carefully; it's the most subtle file in the repo.

### 2c. The handler

`handlers.IngestEvent` — `internal/api/handlers/events.go:34`.

The load-bearing line is `tenantID := auth.TenantFromContext(r.Context())`. **The tenant
comes from the API key, never from the body or URL.** Any `tenant_id` a client sends is
ignored. That's the "override" isolation model, and it's what makes the CORS-enabled
batch endpoint safe — `events.go:92-96` notes that an earlier design taking tenant from
the body turned a batch request into a cross-tenant forgery primitive.

### 2d. The decorator stack

`repo.Create(...)` doesn't reach Postgres directly. From `main.go:164-171`:

```
chaos.EventRepository          ← fault injection (outermost)
  └── resilience.EventRepository   ← circuit breaker + 5s timeout
        └── postgres.EventRepository   ← the real writes
              └── ReplicaPool           ← primary/replica routing + LSN capture
```

Chaos is outermost **on purpose** (`main.go:160-163`): a 15s injected delay exceeds the
inner 5s timeout and trips the breaker, while a 4s delay does not. That's the slow-vs-dead
demo, and it only works in this order. `main.go:250-264` re-applies *both* wrappers when
sharded mode swaps the repo — the comment records that forgetting the chaos wrap was a
real bug where `/api/v1/chaos` kept mutating state while the toggles silently stopped
affecting writes.

### 2e. The write

`postgres.EventRepository.Create` — `internal/storage/postgres/event_repo.go:43`.

One transaction, two tables: `events_store` (canonical, append-only) then `events` (the
synchronous read projection). `event_repo.go:23-27` defends this: it's only the dual-write
antipattern when the destinations are *different storage systems with no joint commit* —
same Postgres means the local transaction gives atomicity for free. Post-commit,
`RecordPostWriteLSN` captures the WAL position for read-your-own-writes.

Kafka publish happens **after** the DB write and is fire-and-forget — a publish failure
does not fail the request (`events.go:72-81`). The batch endpoint goes further and
publishes in a detached goroutine after the response is written (`events.go:238-249`).

### 2f. Async, after the response

The projection runner (`internal/projections/runner.go:79`) polls `events_store` every
second, feeds new events to each projection, and persists position in
`projection_positions` so restarts resume. Projections catch up **independently** — a slow
one doesn't block the others (`runner.go:119-132`), and a failed `Apply` stops that
projection's sweep without advancing past the bad event (`runner.go:150-157`).

`GET /api/v1/projections` reports `head - last_applied`. Note `LagFor` reads head *once*
and reuses it, so a projection can't appear ahead of the head in one snapshot
(`runner.go:174-178`).

---

## 3. The auth model — three tiers

`Router()` splits `/api/v1` into three groups. Which tier a route sits in *is* its
security policy.

| Tier | Guard | Tenant comes from | Example |
|---|---|---|---|
| **Admin** | `Authenticate` + `RequireAdmin` | n/a — cross-tenant | `POST /tenants`, `GET /leader`, `POST /chaos` |
| **Tenant** | `Authenticate` | the API key, from context | `POST /events`, `GET /events/search` |
| **Tenant + path** | `Authenticate` + `EnforceTenantParam` | key, *and* must equal `{tenantID}` in the URL | `GET /tenants/{tenantID}/daily-stats` |

Admin keys carry no tenant and are **rejected** by `EnforceTenantParam` (`auth.go:77-90`)
— admin operates through admin routes, not by impersonating a tenant on path-addressed
ones.

Keys arrive via `X-API-Key`, `Authorization: Bearer`, or `?api_key=`. The query-param path
exists solely for `navigator.sendBeacon`, which cannot set headers (`auth.go:92-99`).
Unknown and revoked keys return **byte-identical** 401s so the API never reveals which
keys exist (`auth.go:50-54`).

### Getting a working credential

1. Set `NEXUS_BOOTSTRAP_ADMIN_KEY` in `.env`. On boot it is hashed and upserted as an
   admin key; the raw value is never logged (`main.go:112-121`).
2. `POST /api/v1/admin/api-keys` with that key to mint a tenant key. **The secret is
   returned exactly once.**
3. Use the tenant key for everything under the tenant tiers.

Without the bootstrap key, admin routes reject every request — in non-production the
server logs a warning saying so.

---

## 4. Endpoint catalogue

Public — no key:

| Method | Path | Notes |
|---|---|---|
| GET | `/health` | liveness |
| GET | `/ready` | pings each wired dependency (`router.go:575`) |
| GET | `/api/v1/metrics` | Prometheus scrape; inside `/api/v1` but outside all auth groups |

**Admin tier** — the "Mounted when" column is the condition in `router.go:456-520`; the
route returns 404 when it's unmet.

| Method | Path | Mounted when |
|---|---|---|
| GET / POST | `/api/v1/tenants` | always |
| POST | `/api/v1/admin/api-keys` | `apiKeys != nil` |
| DELETE | `/api/v1/admin/api-keys/{keyID}` | `apiKeys != nil` |
| GET | `/api/v1/replication-lag` · `/replication-status` | replica pool wired |
| GET | `/api/v1/shard` | shard router ok |
| GET | `/api/v1/shard/distribution` · `/shard/load` | `SHARD_DSN_TEMPLATE` set |
| POST | `/api/v1/billing/charge` | Temporal reachable |
| GET | `/api/v1/circuit-breakers` | always (breakers) |
| GET / POST | `/api/v1/chaos` · POST `/chaos/reset` | chaos profile wired |
| GET / POST | `/api/v1/protected` | fenced resource wired |
| GET | `/api/v1/leader` | **Redis up** |
| GET | `/api/v1/projections` | runner wired — or 503 sentinel in sharded mode |

**Tenant tier** (tenant from key):

| Method | Path |
|---|---|
| POST | `/api/v1/events` · `/api/v1/events/batch` |
| GET | `/api/v1/events` · `/api/v1/events/search?q=` |

**Tenant + path tier**:

| Method | Path | Mounted when |
|---|---|---|
| GET | `/api/v1/tenants/{tenantID}` | always |
| GET | `/api/v1/tenants/{tenantID}/daily-stats` | ClickHouse up |
| GET | `/api/v1/tenants/{tenantID}/live-stats` | Redis up; SSE; gated on `analytics` consent |
| GET | `/api/v1/gdpr/{tenantID}/export` · `/consent` | GDPR wired |
| POST | `/api/v1/gdpr/{tenantID}/erasure` · `/anonymise` · `/consent` | GDPR wired |

**If a route 404s, the service behind it is probably down.** That's the first thing to
check, not a routing bug.

---

## 5. Reading order

Eight files, in order. This is the shortest path to holding the system in your head.

| # | File | Why |
|---|---|---|
| 1 | `cmd/server/main.go` | the entire system as one linear function |
| 2 | `internal/api/router.go` | the route table and the three auth tiers |
| 3 | `internal/api/middleware/auth.go` | the isolation model everything else assumes |
| 4 | `internal/api/handlers/events.go` | the main write path |
| 5 | `internal/storage/postgres/event_repo.go` | the two-table transaction |
| 6 | `internal/eventstore/store.go` | append-only log — note there is **no UPDATE** |
| 7 | `internal/projections/runner.go` | the async catch-up loop |
| 8 | `internal/api/middleware/idempotency.go` | the hardest correctness argument in the repo |

Then pick from §7.

---

## 6. Dependency matrix

Defaults from `internal/config/config.go:92`.

| Service | Env var | Down ⇒ |
|---|---|---|
| **Postgres** | `POSTGRES_DSN` | **fatal** — nothing runs |
| Redis | `REDIS_DSN` | no cache, no leader election, no live-stats route; outbox worker runs unconditionally instead of leader-elected |
| ClickHouse | `CLICKHOUSE_DSN` | no `/daily-stats` |
| Kafka | `KAFKA_BROKERS` | producer connects lazily and never errors at boot; publishes silently fail |
| Temporal | `TEMPORAL_HOST_PORT` | no `/billing/charge` |
| Postgres replica | `POSTGRES_REPLICA_DSN` | primary-only mode, identical behaviour |
| Shards | `SHARD_DSN_TEMPLATE` | router computes indices but every write lands on one Postgres |

Two notes worth carrying:

- `TEMPORAL_HOST_PORT` defaults to `127.0.0.1`, not `localhost`, deliberately
  (`config.go:105-111`): on Windows + Docker Desktop, `localhost` resolves `[::1]` first,
  Temporal's gRPC frontend binds only IPv4, and the IPv6 attempt hangs until timeout —
  surfacing as "temporal: not available" on a perfectly healthy stack.
- In sharded mode the central projection runner is **disabled**, and `/api/v1/projections`
  returns 503 with a reason rather than a misleading `lag=0` (`main.go:355-362`). Each
  shard owns its own `events_store`; reporting the empty central one as healthy would be
  the lie.

---

## 7. The three hard parts

**Fencing tokens** — `internal/election/fenced_resource.go`. Deliberately ~90 lines,
because the idea lives in the protocol, not the storage code: a write is accepted only if
its token strictly exceeds every token applied before. Without this storage-side half,
having the leader carry a token does nothing. Pair `/api/v1/protected` with
`/api/v1/leader`'s `global_fencing_token`. Proof: `make test-fencing` (live Redis).

**Projection rebuild** — `cmd/projection-rebuild`. Resets positions to 0 and replays the
log while the system keeps serving reads. The trade-off is staleness versus downtime, made
explicit.

**GDPR erasure on an append-only log** — `internal/gdpr` + `internal/pii`. PII *does* enter
`events_store` in plaintext: nothing on the ingest path masks the payload (verified by
ingesting a probe event and reading the row back). The masker runs in exactly three places,
all after the fact — `PIIDetect` on the URL/query for logs, `cmd/pii-scanner` over the
events table, and `Service.AnonymiseTenantEvents`. So erasure is resolved by *mutating the
immutable log*: `EraseTenantData` runs `DELETE FROM events_store`, and `AnonymiseTenantEvents`
`UPDATE`s PII to `[REDACTED]` in both `events` and `events_store`.

Anonymising the log too is the load-bearing detail (`service.go:274-282`): scrub only the
`events` projection and a projection rebuild would re-derive the original PII from the log
and undo the erasure. `service.go:283-290` lists the append-only-preserving alternatives —
crypto-shredding, tombstones, compaction-with-rewrite — and states that none is implemented.

---

## 8. Running things

`make run-*` boots exactly the infra a given problem needs and runs the right binary. The
`test-*` and `bench-*` targets are the interesting ones — they assert behaviour rather than
just starting a server:

| Target | Proves |
|---|---|
| `make bench-storage` | same aggregation, Postgres vs ClickHouse (needs `TENANT=<id>`) |
| `make test-anomalies` | lost update, write skew — `internal/transactions/anomalies` |
| `make test-fencing` | token monotonicity; stale-leader writes rejected |
| `make test-batch` | idempotent batch re-run + drift detection |

Standard loop: `make docker-up` → `make migrate-up` → `make seed` → `make run`.
`make load-baseline` / `load-stress` drive the SLO dashboards in `docs/SLOs.md`.

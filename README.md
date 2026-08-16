# Nexus

**A multi-tenant analytics platform, written in Go — built to implement the ideas in
*Designing Data-Intensive Applications* end to end, rather than read them.**

Event ingestion with an append-only event store, CQRS projections that can be rebuilt from
scratch, Redis-based leader election with fencing tokens, Kafka stream processing, batch
aggregation, per-tenant rate limiting, GDPR erasure, PII scanning, and explicit SLOs with
Prometheus/Grafana dashboards calibrated against them.

**~18,750 lines of Go across 22 internal packages, with 37 test files.** It is a learning
system built deliberately and in the open — not a product, and it says so.

---

## Why this exists

Most engineers who say they know distributed systems have read the book. **This is what it
looks like to build it.** Every package below exists because a specific problem — replication
lag, split-brain, exactly-once delivery, schema evolution, the right to erasure — needed a
concrete answer in running code rather than a paragraph.

---

## What's inside, and the problem each piece answers

| Package | The problem it answers |
|---|---|
| `eventstore` | An append-only log as the system of record. Ordering, idempotent appends, replay |
| `projections` | CQRS read models derived from the log — **including rebuilding one from zero** |
| `transactions` | Isolation levels and write conflicts where they actually bite |
| `election` | **Leader election with fencing tokens** — the defence against a paused leader that wakes up and keeps writing |
| `stream` | Kafka consumers, offsets, and what "exactly once" really costs |
| `batch` | Windowed aggregation over the same log the stream reads |
| `storage` | Storage engine trade-offs, benchmarked rather than asserted (`cmd/storage-benchmark`) |
| `encoding` | Schema evolution and backward/forward compatibility |
| `idgen` | Sortable distributed IDs without a coordinator |
| `ratelimit` | Per-tenant limits that survive multiple instances |
| `resilience` · `chaos` | Timeouts, retries, circuit breakers — **and fault injection to prove they work** |
| `auth` · `consent` | API keys hashed at rest, tenant isolation, consent tracking |
| `gdpr` · `pii` · `audit` | Erasure across a log that is supposed to be immutable — the interesting part — plus PII detection (`cmd/pii-scanner`) and an audit trail |
| `billing` | Usage metering via the outbox pattern, with a Temporal workflow |
| `api` · `domain` · `config` · `metrics` | HTTP surface, domain model, configuration, instrumentation |

### The three hardest things in here

1. **Rebuilding a projection from an empty database while the system keeps serving reads.**
   `cmd/projection-rebuild` — the trade-off is staleness versus downtime, and the choice is
   explicit.
2. **Leader election that is safe when a leader stalls.** A lease alone is not enough; a
   paused process can resume believing it still leads. `internal/election` issues monotonic
   **fencing tokens** and storage rejects anything stale.
3. **GDPR erasure against an append-only store.** The right to erasure and an immutable log
   are in direct conflict, and `internal/gdpr` resolves it the blunt way: erasure `DELETE`s
   from the log, anonymisation `UPDATE`s PII to `[REDACTED]` in place. That breaks the
   append-only invariant deliberately — the code says so, and documents the alternatives it
   does not implement (crypto-shredding, tombstones, compaction-with-rewrite).

---

## Running it

**Prerequisites:** Go 1.24+, Docker, `make`, and
[`golang-migrate`](https://github.com/golang-migrate/migrate) on your `PATH` (`make
migrate-up` shells out to it). `k6` only if you want the load targets.

```bash
make docker-up        # postgres + redis
make migrate-up
make seed             # 10k events across 6 tenants; ~30% carry PII for the Ch14 demos
make run              # the API server
make test             # unit tests only — see below
```

Set `NEXUS_BOOTSTRAP_ADMIN_KEY` in `.env` before `make run`, or every `/api/v1` route will
reject you. It is the credential you mint per-tenant keys with; see `.env.example`.

`make test` runs the unit tier. Nine test files are behind `-tags=integration` and need
live services — including all of `internal/transactions`, where the isolation-level
anomalies live. Run those via `make test-fencing`, `make test-batch`, `make test-anomalies`.

`make docker-up-all` brings up the full stack including Kafka UI, schema registry and the
Temporal UI. `make load-baseline` and `make load-stress` drive the SLO dashboards.

**Binaries:** `server` · `stream-processor` · `batch-aggregator` · `projection-rebuild` ·
`storage-benchmark` · `pii-scanner`.
**Also:** a JavaScript SDK in `sdk/js`, and [`docs/SLOs.md`](docs/SLOs.md) — five SLOs the
dashboards and alerts are actually calibrated against.

---

## Status, honestly

**This is a learning system, built to be correct rather than to be operated.** The SLO targets
in `docs/SLOs.md` are course-grade numbers for a single-instance deployment, not production
targets for a multi-region SaaS. There is no HA story, no multi-region anything, and the
Temporal and Kafka setups are single-node.

Saying so is deliberate. **The code defends itself; implying more than it is would not.**

---

## Licence

MIT — see [LICENSE](LICENSE).

*Built by [@mohgh](https://github.com/mohgh) while working through Kleppmann & Riccomini's
*Designing Data-Intensive Applications*. No text from the book is reproduced here.*

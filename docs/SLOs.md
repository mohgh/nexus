# Nexus Service Level Objectives

This document defines the SLOs for Nexus. The numbers below are the
targets the codebase, dashboards, and alerts are calibrated against.
They are **course-grade** numbers chosen to be realistic for a
single-instance educational deployment, not production targets for a
multi-region SaaS.

## SLOs

| ID | Surface | Indicator (SLI) | Objective | Measurement window |
|---|---|---|---|---|
| **S1** | Tenant CRUD (`/api/v1/tenants*`) | request latency | **p99 < 200 ms** | rolling 5 min |
| **S2** | Event ingest (`POST /api/v1/events`) | request latency | **p99 < 500 ms** | rolling 5 min |
| **S3** | Event read/search (`GET /api/v1/events*`) | request latency | **p95 < 300 ms** | rolling 5 min |
| **S4** | All HTTP surfaces | error rate (5xx / total) | **< 0.1%** | multi-window (5m fast / 1h slow) |
| **S5** | All HTTP surfaces | availability (non-5xx / total) | **≥ 99.5%** | rolling 7 day |

S4 and S5 are the same indicator at different timescales. S4 uses
the modern Google SRE multi-window pattern — a 5m window catches
incidents in progress with high signal-to-noise, and a 1h window
catches steady degradation that would still blow the monthly
budget. S5 is the rolling customer-facing budget.

> **Why 7-day budget instead of 30-day?** The math is the same (0.5%
> of all requests), only the observation window is shorter. In an
> educational deployment where `docker compose down -v` is routine,
> a 30-day window almost never accumulates enough data to be
> actionable. 7 days is short enough that the budget rule produces
> meaningful output within a week, long enough to absorb a normal
> incident response cycle. Production SaaS targets would use 28-30
> days.

## Why these numbers

- **Tenant CRUD (S1):** these calls touch a single row by primary key
  and back the management UI. 200 ms is the threshold at which a
  form submission starts to feel sluggish.
- **Event ingest (S2):** the write path crosses Postgres (event_store
  + events dual-write in one tx) and, when sharded, a per-shard
  routing hop. 500 ms accommodates the tx commit and one Kafka
  publish; if p99 grows beyond this the chaos profile (Ch09) or a
  saturated outbox worker (Ch08) is the usual cause.
- **Event read (S3):** reads can hit the replica (Ch06 routing) so
  the bar is tighter than the write path.
- **Error rate (S4):** 0.1% is the threshold that distinguishes
  background noise (a few flaky requests) from a real fault. Most
  client errors (4xx) are not Nexus's fault and are excluded from
  the numerator.
- **Availability (S5):** 99.5% over 7 days = roughly 50 minutes of
  allowed downtime per week. Realistic for an educational deployment
  on a single-host Docker Compose. A production SaaS would target
  99.9–99.99% over 28–30 days.

## Error budget

S5 implies an error budget of 0.5% over 7 days — roughly **50
minutes of full unavailability per week**. The recording rule
`nexus_slo:error_budget_remaining_ratio` (in
`scripts/observability/recording-rules.yml`) computes burn against
this budget. The S4 alerts in the same file fire when the burn rate
would exhaust the budget faster than the window allows (Google SRE
multi-window multi-burn-rate pattern).

## How to verify a change against the SLOs

1. Bring up the observability stack: `make run-observability`.
2. Apply the change.
3. Run a baseline load: `make load-baseline` (k6 script in
   `scripts/load/`).
4. Watch Prometheus or Grafana for `nexus_slo:*` recording rules.
   The dashboards in `scripts/observability/grafana/dashboards/` are
   pre-wired to these rules.
5. If `nexus_slo:p99_latency_seconds{route="..."}` exceeds the
   threshold or `nexus_slo:error_budget_remaining_ratio` drops, the
   change has regressed the SLO. Roll back or accept the regression
   intentionally (and update this doc).

## SLOs are living documents

When a chapter materially changes the request path (e.g., Ch07
adds sharding, Ch08 adds the outbox worker), re-baseline. The
expected slowdown should be quantified in the chapter README and,
if the new p99 violates this SLO, either the SLO is updated
(documented in the chapter) or the chapter explains the
trade-off explicitly.

# Nexus Load Harness

Reproducible load scenarios for the Nexus HTTP API. Targets the SLOs
in [`docs/SLOs.md`](../../docs/SLOs.md) — the script's thresholds
mirror those numbers, so a run that exits non-zero is a regression
against an SLO, not just slow.

## Install k6

| Platform | Command |
|---|---|
| macOS | `brew install k6` |
| Windows | `choco install k6` |
| Linux | follow https://k6.io/docs/get-started/installation/ |
| Anywhere | `docker run -i --rm grafana/k6 run - < baseline.js` |

## Run

```bash
make run-observability   # bring up postgres + redis + observability + server
make load-baseline   # 20 VUs / 3 min — asserts SLO compliance
make load-stress     # 50 VUs / 6 min — explores capacity, soft thresholds
```

Or invoke k6 directly:

```bash
k6 run nexus/scripts/load/baseline.js                          # baseline
NEXUS_LOAD=stress k6 run nexus/scripts/load/baseline.js        # stress
NEXUS_URL=http://staging:8000 k6 run nexus/scripts/load/baseline.js
```

## What the output means

k6 prints latency percentiles per request tag plus pass/fail for each
threshold. Example output of a healthy run:

```
http_req_duration{route:tenant_get}.....: avg=12.4ms p(95)=42ms  p(99)=110ms
http_req_duration{route:event_ingest}...: avg=58.2ms p(95)=210ms p(99)=420ms
http_req_duration{route:event_list}.....: avg=14.1ms p(95)=87ms  p(99)=180ms
http_req_failed.........................: 0.00% ✓ 0 / ✗ 7200

✓ http_req_duration{route:tenant_get}...: p(99)<200    actual=110ms
✓ http_req_duration{route:event_ingest}.: p(99)<500   actual=420ms
✓ http_req_duration{route:event_list}...: p(95)<300   actual=87ms
✓ http_req_failed.......................: rate<0.001  actual=0
```

> **Note on tenant creates:** the SLO threshold targets tenant
> *reads* (`route:tenant_get`), not creates. Tenant POSTs happen
> once in `setup()` (excluded from threshold evaluation) so the
> assertion is computed over thousands of GET samples per run
> rather than the dozen-or-so POSTs that would otherwise dominate
> a baseline. Creates are still load-tested under the stress
> profile, where soft thresholds apply.

A `✗` next to any threshold means the SLO is regressed. Compare
against the per-route panels in the "Nexus — SLO Compliance" Grafana
dashboard to see which percentile actually drifted.

## Cross-referencing with Prometheus

While k6 is running, the Nexus `/metrics` endpoint records every
request server-side too. The recording rules in
[`scripts/observability/recording-rules.yml`](../observability/recording-rules.yml)
compute the same percentiles k6 reports plus the error-budget burn
rate against `slo:error_budget_remaining_ratio`. Use Prometheus when
you want to attribute regressions to a specific tenant or shard;
use k6 when you want a CI-friendly assertion that returns non-zero on
breach.

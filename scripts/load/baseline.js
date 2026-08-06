// Nexus baseline load profile.
//
// Targets the SLOs documented in nexus/docs/SLOs.md. Run with k6:
//
//   k6 run scripts/load/baseline.js
//
// The default profile ramps to 20 virtual users over 30s, sustains for
// 2 minutes, then ramps down. This is a *baseline*, not a stress test —
// it asserts that p99 latency and error rate stay inside the SLO under
// modest concurrent load. For stress / capacity testing, set the env
// var NEXUS_LOAD=stress to bump the VU count.
//
// Sample-count design: tenants are created ONCE in setup() (not per
// VU, not per iteration) so the iteration loop drives meaningful
// sample counts on every threshold. Each iteration does one tenant
// GET, one event POST, and one event LIST — at 20 VUs × ~2 req/s
// each × 180s, that's ~7k samples per route, enough for stable
// percentile estimation.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE = __ENV.NEXUS_URL || 'http://localhost:8000';
const profile = __ENV.NEXUS_LOAD || 'baseline';

export const options = profile === 'stress' ? {
    stages: [
        { duration: '30s', target: 50 },
        { duration: '5m', target: 50 },
        { duration: '30s', target: 0 },
    ],
    thresholds: {
        // Soft thresholds for the stress profile — we expect SLO
        // pressure here, the goal is just to find the breaking point.
        'http_req_failed': ['rate<0.05'],
    },
} : {
    stages: [
        { duration: '30s', target: 20 },
        { duration: '2m', target: 20 },
        { duration: '30s', target: 0 },
    ],
    // Hard thresholds: k6 exits non-zero if these breach. The numbers
    // mirror the SLOs in docs/SLOs.md, so a regression that would
    // page in production fails CI here. Tags map to the per-method
    // route labels Prometheus records server-side, so a k6 breach
    // and a Prom alert fire on the same condition.
    thresholds: {
        // S1: tenant CRUD p99 < 200ms (GET dominates; creates happen
        // once in setup, see below)
        'http_req_duration{route:tenant_get}': ['p(99)<200'],
        // S2: event ingest p99 < 500ms
        'http_req_duration{route:event_ingest}': ['p(99)<500'],
        // S3: event read p95 < 300ms
        'http_req_duration{route:event_list}': ['p(95)<300'],
        // S4: error rate < 0.1%
        'http_req_failed': ['rate<0.001'],
    },
};

// setup runs ONCE on the orchestrator before VUs start. Creating
// tenants here (rather than per-VU in the iteration) means:
//
//   1. Tenant POST samples don't dominate the SLO assertion — the
//      original "one create per VU" approach gave p99 over 20
//      samples, which was statistically meaningless.
//   2. The setup phase is excluded from threshold evaluation (k6
//      contract), so cold-start latency doesn't trigger spurious
//      failures.
//   3. All VUs share a small tenant pool, exercising the cache
//      paths and Postgres connection reuse the way real traffic
//      would.
export function setup() {
    const health = http.get(`${BASE}/health`);
    if (health.status !== 200) {
        throw new Error(`server not reachable at ${BASE} — ${health.status}`);
    }

    // Small pool of 5 tenants. VUs map to tenants deterministically
    // (see default()), so with the baseline's 20 VUs each tenant
    // receives ~4 concurrent VUs hammering it — hot-tenant load
    // rather than uniform-partition. That's a deliberate choice:
    // it stresses cache + connection-pool paths the way a real
    // tenant heatmap does. If you want uniform distribution,
    // bump the pool size to match VU count.
    const tenants = [];
    for (let i = 0; i < 5; i++) {
        const r = http.post(
            `${BASE}/api/v1/tenants/`,
            JSON.stringify({ name: `loadtest-${uuidv4()}`, plan: 'free' }),
            { headers: { 'Content-Type': 'application/json' } },
        );
        if (r.status !== 201) {
            throw new Error(`tenant create failed in setup: ${r.status} ${r.body}`);
        }
        tenants.push(r.json('id'));
    }
    return { tenants };
}

// teardown runs ONCE on the orchestrator after VUs have stopped.
// Without it, every load run would leak 5 fresh tenants into
// Postgres — over a CI loop that's hundreds of orphan rows
// distorting tenant listings and seeded fixtures.
//
// We use the GDPR erasure endpoint rather than a raw DELETE so the
// teardown exercises the same fail-closed deletion path real
// privacy requests use (Ch14). A failure here is logged but
// non-fatal: the k6 run itself already succeeded, we shouldn't
// turn a passing SLO check into a CI failure because cleanup
// hiccuped.
export function teardown(data) {
    for (const tenantID of data.tenants) {
        const r = http.post(`${BASE}/api/v1/gdpr/${tenantID}/erasure`, null);
        if (r.status !== 200 && r.status !== 204) {
            console.warn(`teardown: erasure for ${tenantID} returned ${r.status}`);
        }
    }
}

export default function (data) {
    // Pick a tenant deterministically per-VU (NOT randomly): each
    // VU sticks to one tenant across all its iterations. That
    // produces cache-friendly access patterns within a VU and the
    // hot-tenant overlap across VUs described in setup(). __VU is
    // k6's per-virtual-user counter starting at 1.
    const tenantID = data.tenants[(__VU - 1) % data.tenants.length];

    // S1: tenant GET — exercises the read path, exposes any cache
    // wiring (Ch04). Real CRUD samples land in route:tenant_get,
    // many per iteration across many iterations.
    const get = http.get(
        `${BASE}/api/v1/tenants/${tenantID}`,
        { tags: { route: 'tenant_get' } },
    );
    check(get, { 'tenant GET 200': (r) => r.status === 200 });

    // S2: event ingest — the dominant write path. Idempotency-Key
    // mirrors how real clients should call this endpoint (Ch13).
    const ingest = http.post(
        `${BASE}/api/v1/events`,
        JSON.stringify({
            tenant_id: tenantID,
            event_type: 'page_view',
            payload: { url: '/load-test' },
        }),
        {
            headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': uuidv4(),
            },
            tags: { route: 'event_ingest' },
        },
    );
    check(ingest, { 'event ingested': (r) => r.status === 201 });

    // S3: event read — exercises replica routing if Ch06 is active.
    const list = http.get(
        `${BASE}/api/v1/events?tenant_id=${tenantID}&limit=10`,
        { tags: { route: 'event_list' } },
    );
    check(list, { 'events listed': (r) => r.status === 200 });

    // Pace per VU so we don't accidentally DoS the local server.
    sleep(0.5);
}

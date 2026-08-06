# Nexus Browser SDK

A drop-in `<script>` SDK for ingesting analytics events into Nexus from
a web page. Mirrors the core of PostHog's `posthog-js`.

**Full design rationale** — including why every architectural choice
is what it is — lives in
[`chapters/appendix-a-sdk/README.md`](../../../chapters/appendix-a-sdk/README.md).
This file is the bare quickstart.

## Quickstart

1. Start Nexus (`make run` from `nexus/`), then create a tenant and mint a
   tenant **API key** for it. The API server requires an admin key to mint
   keys — set `NEXUS_BOOTSTRAP_ADMIN_KEY` first (see `.env.example`):
   ```bash
   TENANT=$(curl -s -X POST http://localhost:8000/api/v1/tenants \
     -H "X-API-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
     -d '{"name":"sdk-demo"}' | jq -r .id)

   KEY=$(curl -s -X POST http://localhost:8000/api/v1/admin/api-keys \
     -H "X-API-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
     -d "{\"scope\":\"tenant\",\"tenant_id\":\"$TENANT\",\"name\":\"web\"}" | jq -r .api_key)
   ```

2. Drop the SDK into a page (the key sets the tenant — no tenant id needed):
   ```html
   <script src="nexus.js"></script>
   <script>
     nexus.init({ apiKey: 'nxs_live_…', host: 'http://localhost:8000' });
     nexus.capture('button_clicked', { label: 'signup' });
   </script>
   ```

3. Verify:
   ```bash
   curl -s -H "X-API-Key: $KEY" "http://localhost:8000/api/v1/events?limit=10" | jq
   ```

## Try the demo page

Open `demo.html` directly (`file://` works — `null` is in the default
CORS allow-list) or serve it from any local dev server. Paste a tenant API
key and click around.

## API

| Method | Purpose |
|--------|---------|
| `nexus.init({ apiKey, host, ...opts })` | Configure the SDK. `apiKey` is required; it authenticates the request and sets the tenant. |
| `nexus.capture(eventName, properties)` | Queue an event. Returns immediately; the queue flushes in the background. |
| `nexus.identify(distinctId, personProps)` | Promote the anonymous device to a known user. Sends `$identify` with `$anon_distinct_id` for server-side merge. |
| `nexus.reset()` | Forget the current user. Use on logout. |
| `nexus.register(props)` | Attach properties to *every* subsequent event. |
| `nexus.flush()` | Force the queue to ship now. |

### Config options (`apiKey` required, the rest optional)

| Option | Default | Purpose |
|--------|---------|---------|
| `apiKey` | — | **Required.** Tenant API key (`nxs_live_…`). Authenticates every request and selects the tenant. |
| `host` | `http://localhost:8000` | Nexus API base URL. |
| `batchSize` | `20` | Flush when this many events are queued. |
| `flushIntervalMs` | `5000` | Background flush cadence. |
| `maxQueueSize` | `1000` | Drop oldest events past this cap. |
| `autocapturePageviews` | `true` | Capture `$pageview` on init + SPA route changes. |
| `debug` | `false` | Console-log SDK internals. |

## Server requirements

The SDK assumes Nexus is configured with:

- `POST /api/v1/events/batch` available — wired automatically when the
  event repository is wired.
- A tenant **API key** to authenticate ingest. The SDK sends it as the
  `X-API-Key` header on `fetch` flushes and as `?api_key=` on the
  unload-path `sendBeacon` flush (beacons can't set headers).
- `CORS_ALLOWED_ORIGINS` env var includes the page's origin. The
  default (`http://localhost:3000,http://localhost:5173,http://localhost:8080,null`)
  covers common dev setups + opening `demo.html` directly.

## What's deferred

Listed in priority order in the appendix:

- Click / form autocapture
- Session replay
- Feature flags client
- Server-side identity merge (currently `$identify` events arrive
  with the right metadata but no processor stitches histories)

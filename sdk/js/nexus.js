/*!
 * Nexus Browser SDK
 *
 * A minimal product-analytics SDK that ships events from a web page to a
 * Nexus instance. Mirrors the parts of PostHog's posthog-js that close
 * the "ingest infra has no clients" gap identified in the SDK design doc
 * (see chapters/appendix-a-sdk/README.md).
 *
 * Why every line is here:
 *   - capture() is fire-and-forget from the caller's view but reliable
 *     underneath: events go into an in-memory queue and a sessionStorage
 *     backup so a tab close or reload doesn't drop them.
 *   - The queue flushes on three triggers: batch-size hit, periodic
 *     timer, or page hide. The page-hide path uses sendBeacon because
 *     fetch() can be cancelled by the browser when a tab is closing,
 *     while sendBeacon is contracted to complete.
 *   - identify() promotes the anonymous device_id to a known person.
 *     The previous device_id is sent as $anon_distinct_id on the
 *     identify event so the server can merge histories later.
 *   - Auto $pageview handles both initial load and SPA route changes
 *     (pushState/replaceState monkey-patch + popstate listener), because
 *     single-page apps don't fire navigation events on virtual routes.
 *
 * Storage strategy:
 *   - localStorage: identity (nx_did, nx_uid) + register()'d superProps.
 *     These must be visible across tabs of the same origin.
 *   - sessionStorage: the unflushed event queue. Per-tab, so two tabs
 *     of the same origin don't last-writer-wins each other's queues.
 *     Survives reload, not tab close — acceptable: the page-hide
 *     handler beacons before close, so on-close events are already
 *     in flight before sessionStorage is dropped.
 *
 * Authentication:
 *   Every Nexus API request needs an API key. The key determines the tenant
 *   server-side, so you do NOT send a tenant id — pass `apiKey` to init().
 *   Mint a tenant key with the admin API:
 *     curl -X POST localhost:8000/api/v1/admin/api-keys \
 *       -H "X-API-Key: $ADMIN_KEY" \
 *       -d '{"scope":"tenant","tenant_id":"<uuid>","name":"web"}'
 *   Note: an ingest key shipped in browser JS is public by nature — scope it
 *   to ingest-only and treat it as a write-only token, not a secret.
 *
 * Usage:
 *   <script src="nexus.js"></script>
 *   <script>
 *     nexus.init({ apiKey: 'nxs_live_…', host: 'http://localhost:8000' });
 *     nexus.capture('button_clicked', { label: 'signup' });
 *   </script>
 */
(function (global) {
  'use strict';

  // Storage keys. STORAGE_* are localStorage (cross-tab). SESSION_* are
  // sessionStorage (per-tab).
  var STORAGE_DEVICE_ID = 'nx_did';
  var STORAGE_DISTINCT_ID = 'nx_uid';
  var STORAGE_SUPER_PROPS = 'nx_super';
  var SESSION_QUEUE = 'nx_queue';

  var DEFAULT_CONFIG = {
    host: 'http://localhost:8000',
    apiKey: null,
    tenantId: null, // optional, informational only — the API key sets the tenant
    batchSize: 20,
    flushIntervalMs: 5000,
    maxQueueSize: 1000,
    persistDebounceMs: 250,
    autocapturePageviews: true,
    debug: false
  };

  var state = {
    config: null,
    queue: [],
    seenEventIds: null,    // dedup set for restored queues
    timer: null,
    flushing: false,
    persistTimer: null,
    deviceId: null,
    distinctId: null,
    sessionId: null,
    superProps: {},
    // History API patches install at most once per page. Stored on
    // window.history so a second IIFE evaluation (script loaded twice
    // by hot-reload) doesn't stack patches and double-count pageviews.
    historyPatched: false
  };

  // ─── Public API ────────────────────────────────────────────────────────────

  function init(opts) {
    if (state.config) {
      log('init called twice — ignoring second call');
      return api;
    }
    var cfg = assign({}, DEFAULT_CONFIG, opts || {});
    if (!cfg.apiKey || typeof cfg.apiKey !== 'string') {
      console.error('[nexus] init: apiKey is required and must be a string (mint one via POST /api/v1/admin/api-keys)');
      return api;
    }
    var host = String(cfg.host || '').replace(/\/+$/, '');
    if (!/^https?:\/\//i.test(host)) {
      console.error('[nexus] init: host must be an http(s):// URL, got', cfg.host);
      return api;
    }
    cfg.host = host;
    state.config = cfg;

    state.deviceId = loadOrCreate(STORAGE_DEVICE_ID, generateId);
    state.distinctId = lstorage().getItem(STORAGE_DISTINCT_ID) || state.deviceId;
    state.sessionId = generateId();
    state.superProps = readJSON(lstorage(), STORAGE_SUPER_PROPS) || {};
    state.seenEventIds = new Set();
    restoreQueueFromStorage();

    installLifecycleHandlers();
    if (cfg.autocapturePageviews) installPageviewAutocapture();
    startTimer();

    log('initialised', {
      host: cfg.host,
      tenantId: cfg.tenantId,
      deviceId: state.deviceId,
      distinctId: state.distinctId
    });
    return api;
  }

  // capture queues an event. Properties are merged on top of superProps
  // (set by register()) and a small envelope of context props ($device_id,
  // $session_id, page metadata) so every event self-describes — server-side
  // queries don't have to JOIN against a session table to know the URL.
  function capture(eventName, properties) {
    if (!ensureInit()) return;
    if (!eventName || typeof eventName !== 'string') {
      console.error('[nexus] capture: event name is required');
      return;
    }
    var clientEventId = generateId();
    var envelope = {
      // No tenant_id: the server derives the tenant from the API key.
      event_type: eventName,
      occurred_at: new Date().toISOString(),
      payload: assign(
        {
          $client_event_id: clientEventId,
          $device_id: state.deviceId,
          $distinct_id: state.distinctId,
          $session_id: state.sessionId,
          $lib: 'nexus-js',
          $lib_version: '0.2.0',
          $current_url: location.href,
          $pathname: location.pathname,
          $referrer: document.referrer || null,
          $screen_w: window.screen && window.screen.width,
          $screen_h: window.screen && window.screen.height,
          $user_agent: navigator.userAgent
        },
        state.superProps,
        properties || {}
      )
    };
    if (typeof properties === 'object' && properties && typeof properties.value === 'number') {
      envelope.value = properties.value;
    }
    enqueue(envelope, clientEventId);
  }

  // identify promotes the anonymous device to a known user. The previous
  // distinct_id (typically the device_id) rides along as $anon_distinct_id
  // so a server-side merge can stitch pre-login activity to the account.
  function identify(distinctId, personProperties) {
    if (!ensureInit()) return;
    if (distinctId == null) {
      console.error('[nexus] identify: distinctId is required');
      return;
    }
    if (typeof distinctId !== 'string' && typeof distinctId !== 'number') {
      console.error('[nexus] identify: distinctId must be a string or number, got', typeof distinctId);
      return;
    }
    var id = String(distinctId).trim();
    if (!id) {
      console.error('[nexus] identify: distinctId must be non-empty');
      return;
    }
    var prev = state.distinctId;
    state.distinctId = id;
    lstorage().setItem(STORAGE_DISTINCT_ID, id);
    capture('$identify', assign({}, personProperties || {}, {
      $anon_distinct_id: prev,
      $user_id: id
    }));
  }

  // reset forgets the current identity AND the persisted super-properties
  // (which were probably tied to that user). Use on logout — without it,
  // the next anonymous user on a shared machine inherits the logged-out
  // user's identity AND the server-side merge processor will stitch
  // their activity into the wrong account.
  function reset() {
    if (!ensureInit()) return;
    lstorage().removeItem(STORAGE_DISTINCT_ID);
    lstorage().removeItem(STORAGE_DEVICE_ID);
    lstorage().removeItem(STORAGE_SUPER_PROPS);
    state.superProps = {};
    state.deviceId = loadOrCreate(STORAGE_DEVICE_ID, generateId);
    state.distinctId = state.deviceId;
    state.sessionId = generateId();
  }

  // register attaches properties to every subsequent event AND persists
  // them in localStorage so they survive page reloads (previously they
  // were memory-only, which silently violated the docstring contract).
  function register(props) {
    if (!ensureInit()) return;
    if (!props || typeof props !== 'object') return;
    state.superProps = assign({}, state.superProps, props);
    try { lstorage().setItem(STORAGE_SUPER_PROPS, JSON.stringify(state.superProps)); }
    catch (_) { /* quota exceeded — superProps still live in memory */ }
  }

  function flush() {
    return flushQueue(false);
  }

  // ─── Queue + flush ─────────────────────────────────────────────────────────

  function enqueue(envelope, clientEventId) {
    state.queue.push(envelope);
    if (clientEventId) state.seenEventIds.add(clientEventId);
    if (state.queue.length > state.config.maxQueueSize) {
      // Drop the oldest event to bound memory. Logged at warn so a
      // permanently-broken endpoint is visible rather than silently
      // eating events.
      var dropped = state.queue.shift();
      log('queue full, dropped oldest event', dropped.event_type);
    }
    schedulePersist();
    if (state.queue.length >= state.config.batchSize) {
      flushQueue(false);
    }
  }

  // flushQueue ships the current buffer. When useBeacon=true we're in a
  // page-unload path and must use navigator.sendBeacon — fetch() is liable
  // to be cancelled by the browser as the page tears down, sendBeacon is
  // contracted to complete. Trade-off: sendBeacon can't report success,
  // so we optimistically clear the queue. Acceptable because the worst
  // case is duplicate delivery, and the server's Idempotency middleware
  // collapses dupes when the same Idempotency-Key is replayed (the SDK
  // sends one per flush — on the beacon path via ?idempotency_key=
  // because beacons can't set custom headers).
  function flushQueue(useBeacon) {
    if (!state.config) return Promise.resolve();
    if (state.queue.length === 0) return Promise.resolve();
    if (state.flushing && !useBeacon) return Promise.resolve();

    var batch = state.queue.slice();
    var url = state.config.host + '/api/v1/events/batch';
    // No tenant_id in the body: the server derives the tenant from the
    // API key. Sending one would be ignored.
    var body = JSON.stringify({ events: batch });
    var idempotencyKey = generateId();

    if (useBeacon && navigator.sendBeacon) {
      // sendBeacon can't set custom headers, so both the idempotency key
      // and the API key ride in the query string on the unload path. The
      // server reads ?idempotency_key= and ?api_key= as fallbacks. Without
      // the api_key fallback the beacon flush would 401; without
      // idempotency_key, visibilitychange + pagehide firing in sequence
      // produces duplicate ingest.
      var beaconURL = url +
        '?idempotency_key=' + encodeURIComponent(idempotencyKey) +
        '&api_key=' + encodeURIComponent(state.config.apiKey);
      var blob = new Blob([body], { type: 'application/json' });
      var ok = navigator.sendBeacon(beaconURL, blob);
      if (ok) {
        consumeBatch(batch.length);
      }
      return Promise.resolve();
    }

    state.flushing = true;
    return fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
        'X-API-Key': state.config.apiKey
      },
      body: body,
      keepalive: true,
      credentials: 'omit'
    })
      .then(function (res) {
        // 2xx and 207 (Multi-Status, partial success) both clear the
        // batch — per-event errors are inspected via the response body
        // by the caller if they care, but a poison event must not block
        // every subsequent flush.
        if (res.ok || res.status === 207) {
          consumeBatch(batch.length);
          log('flushed', batch.length, 'events');
        } else {
          log('flush HTTP error', res.status, '— keeping batch for retry');
        }
      })
      .catch(function (err) {
        // Network error: leave the batch in the queue. The next timer
        // tick or capture() that triggers a batch flush will retry.
        log('flush failed', err && err.message, '— keeping batch for retry');
      })
      .then(function () { state.flushing = false; });
  }

  // consumeBatch removes the first N events from the queue and drops
  // their dedup IDs. Used by both fetch-success and beacon-success
  // paths so they stay in lockstep about what's been shipped.
  function consumeBatch(n) {
    var removed = state.queue.splice(0, n);
    for (var i = 0; i < removed.length; i++) {
      var id = removed[i].payload && removed[i].payload.$client_event_id;
      if (id) state.seenEventIds.delete(id);
    }
    schedulePersist();
  }

  function startTimer() {
    if (state.timer) clearInterval(state.timer);
    state.timer = setInterval(function () { flushQueue(false); }, state.config.flushIntervalMs);
  }

  function stopTimer() {
    if (state.timer) { clearInterval(state.timer); state.timer = null; }
    if (state.persistTimer) {
      clearTimeout(state.persistTimer);
      state.persistTimer = null;
      persistQueueToStorage(); // flush the pending write before we stop
    }
  }

  // ─── Page lifecycle ────────────────────────────────────────────────────────

  function installLifecycleHandlers() {
    // visibilitychange fires more reliably than beforeunload on mobile
    // browsers (which often kill background tabs without firing unload).
    // We flush on both for belt-and-braces coverage.
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'hidden') flushQueue(true);
    });
    window.addEventListener('pagehide', function () {
      flushQueue(true);
      stopTimer();
    });
  }

  // installPageviewAutocapture wires:
  //   - one $pageview at init time
  //   - one on browser back/forward (popstate)
  //   - one on programmatic SPA navigation (pushState / replaceState)
  // The pushState wrap is the part most ad-hoc analytics snippets miss
  // and is what makes SPAs (React/Vue/Svelte routers) actually track.
  //
  // The patch is gated on a sentinel on the History object so reloading
  // the SDK script (dev hot-reload) doesn't stack patches and double
  // every subsequent pageview.
  function installPageviewAutocapture() {
    capture('$pageview', { $title: document.title });

    window.addEventListener('popstate', function () {
      capture('$pageview', { $title: document.title, $navigation: 'popstate' });
    });

    if (history.__nx_patched) return;
    history.__nx_patched = true;

    var origPush = history.pushState;
    var origReplace = history.replaceState;
    history.pushState = function () {
      var rv = origPush.apply(this, arguments);
      try { capture('$pageview', { $title: document.title, $navigation: 'pushState' }); }
      catch (e) { log('pageview capture (pushState) threw', e && e.message); }
      return rv;
    };
    history.replaceState = function () {
      var rv = origReplace.apply(this, arguments);
      try { capture('$pageview', { $title: document.title, $navigation: 'replaceState' }); }
      catch (e) { log('pageview capture (replaceState) threw', e && e.message); }
      return rv;
    };
  }

  // ─── Storage helpers ───────────────────────────────────────────────────────

  // lstorage / sstorage fall back to an in-memory shim when the real
  // storage is unavailable (private browsing, iframe with storage
  // disabled). The SDK still works for the lifetime of the page; it
  // just can't survive a reload.
  var lmemShim = null;
  var smemShim = null;
  function lstorage() { return getStorage('localStorage', function () { return lmemShim; }, function (s) { lmemShim = s; }); }
  function sstorage() { return getStorage('sessionStorage', function () { return smemShim; }, function (s) { smemShim = s; }); }
  function getStorage(name, getShim, setShim) {
    try {
      var real = window[name];
      if (real) {
        var k = '__nx_probe__';
        real.setItem(k, '1');
        real.removeItem(k);
        return real;
      }
    } catch (_) { /* fall through */ }
    var existing = getShim();
    if (existing) return existing;
    var mem = {};
    var shim = {
      getItem: function (k) { return Object.prototype.hasOwnProperty.call(mem, k) ? mem[k] : null; },
      setItem: function (k, v) { mem[k] = String(v); },
      removeItem: function (k) { delete mem[k]; }
    };
    setShim(shim);
    return shim;
  }

  function loadOrCreate(key, generator) {
    var v = lstorage().getItem(key);
    if (v) return v;
    v = generator();
    lstorage().setItem(key, v);
    return v;
  }

  function readJSON(store, key) {
    try {
      var raw = store.getItem(key);
      return raw ? JSON.parse(raw) : null;
    } catch (_) { return null; }
  }

  // schedulePersist debounces queue writes so a burst of N click events
  // produces one localStorage write instead of N. Persistence on every
  // capture() defeats the SDK's "return in microseconds" design goal.
  function schedulePersist() {
    if (state.persistTimer) return;
    state.persistTimer = setTimeout(function () {
      state.persistTimer = null;
      persistQueueToStorage();
    }, state.config.persistDebounceMs);
  }

  function persistQueueToStorage() {
    try {
      sstorage().setItem(SESSION_QUEUE, JSON.stringify(state.queue));
    } catch (_) {
      // Quota exceeded or storage unavailable. The queue still lives
      // in memory; we just lose the durability across reload.
    }
  }

  // restoreQueueFromStorage reads sessionStorage (per-tab), dedups by
  // $client_event_id against any events captured in the new page so a
  // double-init doesn't replay the same event twice.
  function restoreQueueFromStorage() {
    var parsed = readJSON(sstorage(), SESSION_QUEUE);
    if (!Array.isArray(parsed) || !parsed.length) return;
    var deduped = [];
    for (var i = 0; i < parsed.length; i++) {
      var ev = parsed[i];
      var id = ev && ev.payload && ev.payload.$client_event_id;
      if (id && state.seenEventIds.has(id)) continue;
      if (id) state.seenEventIds.add(id);
      deduped.push(ev);
    }
    if (deduped.length) {
      state.queue = deduped.concat(state.queue);
      log('restored', deduped.length, 'events from storage');
    }
  }

  // ─── Tiny utilities ────────────────────────────────────────────────────────

  function generateId() {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    // RFC4122 v4-ish fallback. Good enough for IDs that don't need
    // cryptographic uniqueness across the entire internet.
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      var v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  function assign(target) {
    for (var i = 1; i < arguments.length; i++) {
      var src = arguments[i];
      if (!src) continue;
      for (var key in src) {
        if (Object.prototype.hasOwnProperty.call(src, key)) target[key] = src[key];
      }
    }
    return target;
  }

  function ensureInit() {
    if (!state.config) {
      console.error('[nexus] call nexus.init({tenantId,host}) before any capture/identify');
      return false;
    }
    return true;
  }

  function log() {
    if (!state.config || !state.config.debug) return;
    var args = Array.prototype.slice.call(arguments);
    args.unshift('[nexus]');
    console.log.apply(console, args);
  }

  // ─── Export ────────────────────────────────────────────────────────────────

  var api = {
    init: init,
    capture: capture,
    identify: identify,
    reset: reset,
    register: register,
    flush: flush,
    get deviceId() { return state.deviceId; },
    get distinctId() { return state.distinctId; },
    get sessionId() { return state.sessionId; },
    get queueLength() { return state.queue.length; }
  };

  // Guard against repeated IIFE evaluation in dev (hot-reload, script
  // included twice). The first load wins; subsequent loads keep the
  // existing api object so callers don't lose their reference.
  if (!global.nexus) global.nexus = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = global.nexus;
})(typeof window !== 'undefined' ? window : this);

'use strict';
/*
 * DevTools panel transport: long-lived port to bg.js, scoped to the inspected
 * tab. MV3 service workers stop after ~30s idle and cold starts can race the
 * init/hello handshake (silently, without onDisconnect), so this transport:
 *  - retries with backoff when hello doesn't arrive in time
 *  - pings the port while the panel is open, keeping the SW (and its ring
 *    buffer) alive
 *  - catches up via timestamp on reconnect instead of dropping the replay
 * Background seq counters reset when the SW restarts, so events are
 * renumbered locally to keep seq keys unique.
 */

let localSeq = 0;
let lastTs = 0; // newest applied event timestamp — dedupes reconnect replays
let retryMs = 500;
let retryTimer = null;

function take(e) {
  e.seq = ++localSeq;
  if (e.ts > lastTs) lastTs = e.ts;
  ingest(e);
}

function scheduleRetry() {
  if (retryTimer) return;
  retryTimer = setTimeout(() => { retryTimer = null; connectBg(); }, retryMs);
  retryMs = Math.min(retryMs * 2, 5000);
}

function connectBg() {
  let port;
  try {
    port = chrome.runtime.connect({ name: 'wsview-panel' });
  } catch {
    setStatus(false);
    scheduleRetry();
    return;
  }

  let alive = true;
  let gotHello = false;
  const keepalive = setInterval(() => {
    try { port.postMessage({ t: 'ping' }); } catch { /* onDisconnect cleans up */ }
  }, 20000);
  const teardown = () => {
    alive = false;
    clearInterval(keepalive);
    setStatus(false);
    scheduleRetry();
  };
  const helloTimeout = setTimeout(() => {
    if (!gotHello && alive) {
      // cold-start race: the SW never answered — tear down and retry
      try { port.disconnect(); } catch { /* already gone */ }
      teardown();
    }
  }, 1500);

  port.onMessage.addListener((e) => {
    if (e.t === 'hello') {
      gotHello = true;
      retryMs = 500;
      setStatus(true);
      const fresh = e.events.filter((x) => x.ts > lastTs);
      if (fresh.length) {
        S.batch = true;
        for (const x of fresh) take(x);
        S.batch = false;
        updateConnSelect();
        updateCounts();
        rebuild();
      }
      return;
    }
    if (e.t === 'pong') return;
    take(e);
  });
  port.onDisconnect.addListener(() => {
    if (!alive) return; // manual teardown already scheduled a retry
    clearTimeout(helloTimeout);
    teardown();
  });
  port.postMessage({ t: 'init', tabId: chrome.devtools.inspectedWindow.tabId });
}

document.querySelector('#target').textContent = `page WebSocket traffic · tab ${chrome.devtools.inspectedWindow.tabId}`;
connectBg();

'use strict';
/*
 * Isolated-world content script: relays events posted by inject.js (MAIN world)
 * to the background service worker over a long-lived port.
 */
let port = null;

function ensurePort() {
  if (port) return port;
  try {
    port = chrome.runtime.connect({ name: 'wsview-cs' });
    port.onDisconnect.addListener(() => { port = null; });
  } catch {
    port = null; // extension reloaded/disabled — page keeps working, we just stop relaying
  }
  return port;
}

function send(m) {
  const p = ensurePort();
  if (!p) return;
  try { p.postMessage(m); } catch { port = null; }
}

// While the page actively uses WebSockets, ping the service worker so it (and
// its history buffer) stays alive; stop 5 minutes after the last frame so
// quiet tabs don't keep the SW awake forever.
let lastEvt = 0;
let keepalive = null;
function bump() {
  lastEvt = Date.now();
  if (keepalive) return;
  keepalive = setInterval(() => {
    if (Date.now() - lastEvt > 5 * 60 * 1000) {
      clearInterval(keepalive);
      keepalive = null;
      return;
    }
    send({ t: 'ka' });
  }, 20000);
}

window.addEventListener('message', (ev) => {
  if (ev.source !== window || !ev.data || !ev.data.__wsview) return;
  send(ev.data.__wsview);
  bump();
});

// marks reloads/navigations in the stream (top frame only, to avoid iframe noise)
if (window === window.top) send({ t: 'nav', ts: Date.now(), url: location.href });

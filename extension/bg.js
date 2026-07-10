'use strict';
/*
 * Background service worker: hub between content-script relays and DevTools
 * panels. Keeps a small per-tab ring buffer so a panel opened after traffic
 * started still sees recent history (best effort — the buffer lives in SW
 * memory and is lost if Chrome suspends the worker during a quiet stretch).
 */

const RING = 2000;
const RING_BYTES = 50 * 1024 * 1024;

const tabs = new Map(); // tabId -> { seq, ring: [], bytes, panels: Set<Port> }

function tabState(tabId) {
  let st = tabs.get(tabId);
  if (!st) {
    st = { seq: 0, ring: [], bytes: 0, panels: new Set() };
    tabs.set(tabId, st);
  }
  return st;
}

function push(tabId, e) {
  const st = tabState(tabId);
  e.seq = ++st.seq;
  st.ring.push(e);
  st.bytes += (typeof e.data === 'string' ? e.data.length : 0) + 200;
  while (st.ring.length > RING || st.bytes > RING_BYTES) {
    const d = st.ring.shift();
    if (!d) break;
    st.bytes -= (typeof d.data === 'string' ? d.data.length : 0) + 200;
  }
  for (const p of st.panels) {
    try { p.postMessage(e); } catch { st.panels.delete(p); }
  }
}

chrome.runtime.onConnect.addListener((port) => {
  if (port.name === 'wsview-cs') {
    const tabId = port.sender?.tab?.id;
    if (tabId === undefined) return;
    const frameId = port.sender.frameId || 0;
    const conn = (sock) => (frameId ? `${frameId}:${sock}` : sock);
    port.onMessage.addListener((m) => {
      if (m.t === 'nav') {
        push(tabId, { t: 'nav', ts: m.ts, conn: 0, url: m.url });
      } else if (m.t === 'open') {
        push(tabId, { t: 'open', ts: m.ts, conn: conn(m.sock), path: m.url, upstream: m.protocol ? `subprotocol: ${m.protocol}` : '' });
      } else if (m.t === 'close') {
        push(tabId, { t: 'close', ts: m.ts, conn: conn(m.sock), code: m.code, reason: m.reason || '', ms: m.ms || 0 });
      } else if (m.t === 'error') {
        push(tabId, { t: 'error', ts: m.ts, conn: conn(m.sock), message: m.message });
      } else if (m.t === 'msg') {
        // classification happens in the panel (viewer.js); truncated frames
        // carry meta parsed by inject.js from the full text
        const e = { t: 'msg', ts: m.ts, conn: conn(m.sock), dir: m.dir, size: m.size, binary: m.binary, data: m.data };
        if (m.truncated) e.truncated = true;
        if (m.meta) e.meta = m.meta;
        push(tabId, e);
      }
    });
    return;
  }

  if (port.name === 'wsview-panel') {
    let tabId;
    port.onMessage.addListener((m) => {
      if (m.t === 'ping') {
        // panel keepalive: the message itself resets the SW idle timer
        try { port.postMessage({ t: 'pong' }); } catch { /* port gone */ }
        return;
      }
      if (m.t === 'init' && typeof m.tabId === 'number') {
        tabId = m.tabId;
        const st = tabState(tabId);
        st.panels.add(port);
        try { port.postMessage({ t: 'hello', events: st.ring }); } catch { st.panels.delete(port); }
      }
    });
    port.onDisconnect.addListener(() => {
      if (tabId !== undefined) tabState(tabId).panels.delete(port);
    });
  }
});

chrome.tabs.onRemoved.addListener((tabId) => tabs.delete(tabId));

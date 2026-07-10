'use strict';
/*
 * MAIN-world content script (document_start): wraps window.WebSocket so every
 * socket the page creates is mirrored via window.postMessage to relay.js.
 * Frames are forwarded to the page/server untouched.
 */
(() => {
  if (window.__wsviewPatched) return;
  window.__wsviewPatched = true;
  const Native = window.WebSocket;
  if (!Native) return;

  const STORE_CAP = 1024 * 1024; // chars kept per frame for display
  let sockSeq = 0;
  const ids = new WeakMap();

  const post = (m) => {
    try { window.postMessage({ __wsview: m }, '*'); } catch { /* unclonable — drop */ }
  };

  // used only for frames too large to ship whole, so the panel can still
  // classify and pair them; keep in sync with classify() in viewer.js
  function rpcShape(o) {
    if (typeof o.method === 'string') {
      return o.id === undefined || o.id === null
        ? { kind: 'notification', method: o.method }
        : { kind: 'request', method: o.method, id: o.id };
    }
    if ('id' in o && ('result' in o || 'error' in o)) {
      if ('error' in o) {
        const err = o.error ?? {};
        return { kind: 'error', id: o.id, errCode: typeof err.code === 'number' ? err.code : undefined, errMsg: err.message === undefined ? undefined : String(err.message) };
      }
      return { kind: 'response', id: o.id };
    }
    return null;
  }
  function parseMeta(text) {
    try {
      const j = JSON.parse(text);
      if (Array.isArray(j)) return { kind: 'batch', count: j.length };
      if (j === null || typeof j !== 'object') return { kind: 'json' };
      const direct = rpcShape(j);
      if (direct) return direct;
      for (const k of ['payload', 'data', 'message', 'body']) {
        const inner = j[k];
        if (inner && typeof inner === 'object' && !Array.isArray(inner) && inner.jsonrpc === '2.0') {
          const m = rpcShape(inner);
          if (m) {
            const lk = ['type', 'event', 'action', 'kind', 'op', 'topic', 'channel'].find((x) => typeof j[x] === 'string');
            m.env = lk ? j[lk] : k;
            return m;
          }
        }
      }
      return { kind: 'json' };
    } catch {
      return { kind: 'text' };
    }
  }

  function payload(data) {
    if (typeof data === 'string') {
      if (data.length > STORE_CAP) {
        return { size: data.length, binary: false, data: data.slice(0, STORE_CAP), truncated: true, meta: parseMeta(data) };
      }
      return { size: data.length, binary: false, data };
    }
    let head = null, size = 0;
    if (data instanceof ArrayBuffer) {
      size = data.byteLength;
      head = new Uint8Array(data, 0, Math.min(64, size));
    } else if (ArrayBuffer.isView(data)) {
      size = data.byteLength;
      head = new Uint8Array(data.buffer, data.byteOffset, Math.min(64, size));
    } else if (typeof Blob !== 'undefined' && data instanceof Blob) {
      size = data.size;
    }
    const hex = head ? Array.from(head, (b) => b.toString(16).padStart(2, '0')).join('') : '';
    return { size, binary: true, data: hex };
  }

  function tap(ws, url) {
    const sock = ++sockSeq;
    const t0 = Date.now();
    ids.set(ws, sock);
    ws.addEventListener('open', () => post({ t: 'open', sock, ts: Date.now(), url: String(url), protocol: ws.protocol }));
    ws.addEventListener('message', (ev) => post({ t: 'msg', sock, ts: Date.now(), dir: 's2c', ...payload(ev.data) }));
    ws.addEventListener('close', (ev) => post({ t: 'close', sock, ts: Date.now(), code: ev.code, reason: ev.reason, ms: Date.now() - t0 }));
    ws.addEventListener('error', () => post({ t: 'error', sock, ts: Date.now(), message: 'socket error' }));
  }

  window.WebSocket = new Proxy(Native, {
    construct(target, args, newTarget) {
      const ws = Reflect.construct(target, args, newTarget);
      try { tap(ws, args[0]); } catch { /* never break the page */ }
      return ws;
    },
  });

  // patch the prototype too: catches WebSocket.prototype.send.call(ws, …) and
  // instances created before page scripts could observe the wrapped constructor
  const nativeSend = Native.prototype.send;
  Native.prototype.send = function send(data) {
    const sock = ids.get(this);
    if (sock !== undefined) {
      try { post({ t: 'msg', sock, ts: Date.now(), dir: 'c2s', ...payload(data) }); } catch { /* never break the page */ }
    }
    return nativeSend.call(this, data);
  };
})();

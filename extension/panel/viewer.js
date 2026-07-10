'use strict';
/*
 * wsview viewer core — transport-agnostic. Shared by the web UI (app.js, feed
 * WebSocket) and the Chrome extension DevTools panel (panel.js, runtime port).
 * Transports call: ingest, resetState, rebuild, updateConnSelect, updateCounts, setStatus.
 * After editing this file run `npm run sync:ext` to refresh the extension copy.
 */

const tbody = document.querySelector('#rows');
const wrap = document.querySelector('#tablewrap');
const detail = document.querySelector('#detail');
const newpill = document.querySelector('#newpill');
const countsEl = document.querySelector('#counts');
const pauseBtn = document.querySelector('#pause');

const MAX_EVENTS = 5000;

const S = {
  events: [],
  bySeq: new Map(),
  pending: new Map(),   // `${conn}|${dir}|${idKey}` -> request evt, for pairing
  conns: new Map(),     // conn -> { path, open }
  kinds: new Set(['request', 'response', 'notification', 'error', 'other']),
  text: '',
  conn: 'all',
  paused: false,
  pausedCount: 0,
  selected: null,
  follow: true,
  newCount: 0,
  batch: false,
  counts: { total: 0, request: 0, response: 0, notification: 0, error: 0, other: 0 },
};

// ---------- helpers ----------

const pad = (n, w) => String(n).padStart(w, '0');
const fmtTime = (ts) => {
  const d = new Date(ts);
  return `${pad(d.getHours(), 2)}:${pad(d.getMinutes(), 2)}:${pad(d.getSeconds(), 2)}.${pad(d.getMilliseconds(), 3)}`;
};
const fmtMs = (n) => (n < 1 ? '<1 ms' : n < 1000 ? `${n} ms` : `${(n / 1000).toFixed(2)} s`);
const fmtSize = (n) => (n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(2)} MB`);
const fmtDur = (ms) => (ms < 1000 ? `${ms} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`);
const opp = (dir) => (dir === 'c2s' ? 's2c' : 'c2s');
const idKey = (id) => `${typeof id}:${String(id)}`;

const el = (tag, cls, text) => {
  const x = document.createElement(tag);
  if (cls) x.className = cls;
  if (text !== undefined) x.textContent = text;
  return x;
};

const BADGE = {
  request: 'REQ', response: 'RES', notification: 'NTF', error: 'ERR',
  binary: 'BIN', text: 'TXT', json: 'JSON', batch: 'BAT', open: 'OPEN', close: 'CLOSE', nav: 'NAV',
};
const badge = (kind) => el('span', `badge k-${kind}`, BADGE[kind] ?? String(kind).toUpperCase());

function kindGroup(e) {
  const k = e.meta?.kind;
  return k === 'request' || k === 'response' || k === 'notification' || k === 'error' ? k : 'other';
}

// ---------- classification ----------
// Frames usually arrive without meta and are classified here. Exception:
// frames too large to ship whole are classified at capture time (server.ts /
// inject.js) from the full text, and that meta is trusted as-is.

const WRAP_KEYS = ['payload', 'data', 'message', 'body'];
const LABEL_KEYS = ['type', 'event', 'action', 'kind', 'op', 'topic', 'channel'];

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

function envLabel(o, wrapKey) {
  for (const k of LABEL_KEYS) if (typeof o[k] === 'string') return o[k];
  return wrapKey;
}

function jsonLabel(o) {
  for (const k of LABEL_KEYS) if (typeof o[k] === 'string') return `${k}=${o[k]}`;
  const keys = Object.keys(o);
  return `{${keys.slice(0, 4).join(', ')}${keys.length > 4 ? ', …' : ''}}`;
}

function classify(e) {
  if (e.binary) return { kind: 'binary' };
  let j;
  try { j = JSON.parse(e.data); } catch { return { kind: 'text' }; }
  if (Array.isArray(j)) return { kind: 'batch', count: j.length };
  if (j === null || typeof j !== 'object') return { kind: 'json' };
  const direct = rpcShape(j);
  if (direct) return direct;
  // enveloped JSON-RPC, e.g. {type:"control", payload:{jsonrpc:"2.0", method:…}};
  // the inner object must declare jsonrpc to avoid false positives
  for (const k of WRAP_KEYS) {
    const inner = j[k];
    if (inner && typeof inner === 'object' && !Array.isArray(inner) && inner.jsonrpc === '2.0') {
      const m = rpcShape(inner);
      if (m) { m.env = envLabel(j, k); return m; }
    }
  }
  return { kind: 'json', label: jsonLabel(j) };
}

// ---------- filtering ----------

function passes(e) {
  if (S.conn !== 'all' && String(e.conn) !== S.conn) return false;
  if (e.t !== 'msg') return !S.text; // connection events stay visible unless text-filtering
  if (!S.kinds.has(kindGroup(e))) return false;
  if (S.text) {
    const hay = `${e.method ?? ''} ${e.meta?.method ?? ''} ${e.meta?.id ?? ''} ${e.data}`.toLowerCase();
    if (!hay.includes(S.text)) return false;
  }
  return true;
}

// ---------- rows ----------

function sysText(e) {
  if (e.t === 'open') {
    const arrow = e.upstream && e.upstream !== e.path ? ` → ${e.upstream}` : '';
    return `● conn #${e.conn} opened   ${e.path}${arrow}`;
  }
  if (e.t === 'close') {
    const reason = e.reason ? ' ' + JSON.stringify(e.reason) : '';
    const by = e.by ? ` · by ${e.by}` : '';
    const after = e.ms ? ` · after ${fmtDur(e.ms)}` : '';
    return `○ conn #${e.conn} closed   code ${e.code}${reason}${by}${after}`;
  }
  if (e.t === 'nav') return `⟳ ${e.url}`;
  return `⚠ conn #${e.conn} ${e.side ?? ''} error: ${e.message}`;
}

function methodTd(e, m) {
  const td = el('td', 'method');
  if (m.env) td.append(el('span', 'envtag', `${m.env} · `));
  if (m.kind === 'request' || m.kind === 'notification') {
    td.append(el('span', '', m.method ?? ''));
  } else if (m.kind === 'response') {
    td.append(el('span', e.method ? '' : 'preview', e.method ?? '(response)'));
  } else if (m.kind === 'error') {
    if (e.method) td.append(el('span', '', e.method + ' '));
    td.append(el('span', 'errinfo', `✕ ${m.errCode ?? ''} ${m.errMsg ?? ''}`));
  } else if (m.kind === 'binary') {
    td.append(el('span', 'preview', `0x${e.data.slice(0, 32)}…`));
  } else if (m.kind === 'batch') {
    td.append(el('span', 'preview', `batch[${m.count}] ${e.data.slice(0, 60)}`));
  } else if (m.kind === 'json' && m.label) {
    td.append(el('span', '', m.label));
  } else {
    td.append(el('span', 'preview', e.data.slice(0, 100)));
  }
  return td;
}

function renderRow(e) {
  const tr = document.createElement('tr');
  tr.dataset.seq = e.seq;
  if (S.selected === e.seq) tr.classList.add('sel');
  if (e.t !== 'msg') {
    tr.classList.add('sys');
    if (e.t === 'error') tr.classList.add('syserr');
    const td = el('td', '', sysText(e));
    td.colSpan = 8;
    tr.append(td);
  } else {
    const m = e.meta ?? { kind: 'text' };
    tr.classList.add(`g-${kindGroup(e)}`);
    const dirTd = el('td', `dir ${e.dir}`, e.dir === 'c2s' ? '→' : '←');
    dirTd.title = e.dir === 'c2s' ? 'client → server' : 'server → client';
    tr.append(
      el('td', 'time', fmtTime(e.ts)),
      dirTd,
      el('td', 'conn', `#${e.conn}`),
      (() => { const td = el('td'); td.append(badge(m.kind)); return td; })(),
      methodTd(e, m),
      el('td', 'id', m.id === undefined || m.id === null ? '' : String(m.id)),
      el('td', 'lat', e.latency !== undefined ? fmtMs(e.latency) : ''),
      el('td', 'size', fmtSize(e.size)),
    );
  }
  tr.addEventListener('click', () => select(e.seq));
  return tr;
}

function appendRow(e) {
  if (!passes(e)) return;
  tbody.appendChild(renderRow(e));
  if (S.follow) {
    wrap.scrollTop = wrap.scrollHeight;
  } else {
    S.newCount++;
    newpill.textContent = `↓ ${S.newCount} new`;
    newpill.hidden = false;
  }
}

function removeRow(seq) {
  tbody.querySelector(`tr[data-seq="${seq}"]`)?.remove();
}

function patchRow(req) {
  const row = tbody.querySelector(`tr[data-seq="${req.seq}"]`);
  if (row) {
    const lat = row.querySelector('td.lat');
    if (lat) lat.textContent = fmtMs(req.latency);
  }
  if (S.selected === req.seq) renderDetail(req);
}

function rebuild() {
  const frag = document.createDocumentFragment();
  for (const e of S.events) if (passes(e)) frag.appendChild(renderRow(e));
  tbody.replaceChildren(frag);
  if (S.follow) wrap.scrollTop = wrap.scrollHeight;
}

// ---------- ingest & pairing ----------

function bumpCounts(e) {
  if (e.t !== 'msg') return;
  S.counts.total++;
  S.counts[kindGroup(e)]++;
}

function updateCounts() {
  const c = S.counts;
  countsEl.innerHTML = `${c.total} msgs · ${c.request} req · ${c.response} res · ${c.notification} notif · ` +
    `<span class="${c.error ? 'errtxt' : ''}">${c.error} err</span>`;
}

function updateConnSelect() {
  if (S.batch) return;
  const sel = document.querySelector('#connSel');
  const cur = sel.value;
  const opts = ['<option value="all">all conns</option>'];
  for (const [id, c] of S.conns) {
    opts.push(`<option value="${id}">#${id} ${String(c.path).replace(/[<>&"]/g, '')}${c.open ? '' : ' (closed)'}</option>`);
  }
  sel.innerHTML = opts.join('');
  sel.value = [...sel.options].some((o) => o.value === cur) ? cur : 'all';
}

function ingest(e) {
  if (e.t === 'open') { S.conns.set(e.conn, { path: e.path, open: true }); updateConnSelect(); }
  if (e.t === 'close') { const c = S.conns.get(e.conn); if (c) c.open = false; updateConnSelect(); }

  if (e.t === 'msg' && !e.meta) e.meta = classify(e);
  if (e.t === 'msg' && e.meta) {
    const m = e.meta;
    if (m.kind === 'request' && m.id !== undefined && m.id !== null) {
      S.pending.set(`${e.conn}|${e.dir}|${idKey(m.id)}`, e);
    } else if ((m.kind === 'response' || m.kind === 'error') && m.id !== undefined && m.id !== null) {
      // the matching request travelled in the opposite direction (agent protocols are bidirectional)
      const key = `${e.conn}|${opp(e.dir)}|${idKey(m.id)}`;
      const req = S.pending.get(key);
      if (req) {
        S.pending.delete(key);
        e.reqSeq = req.seq;
        e.latency = e.ts - req.ts;
        e.method = m.method ?? req.meta.method;
        req.resSeq = e.seq;
        req.latency = e.latency;
        if (!S.batch && !S.paused) patchRow(req);
      }
    }
  }

  S.events.push(e);
  S.bySeq.set(e.seq, e);
  bumpCounts(e);
  if (S.events.length > MAX_EVENTS) {
    const dropped = S.events.shift();
    S.bySeq.delete(dropped.seq);
    removeRow(dropped.seq);
    if (S.selected === dropped.seq) deselect();
  }
  if (S.batch) return;
  updateCounts();
  if (S.paused) {
    S.pausedCount++;
    pauseBtn.textContent = `▶ resume (${S.pausedCount})`;
    return;
  }
  appendRow(e);
}

// ---------- detail pane ----------

function labelFor(e) {
  const k = e.meta?.kind;
  if (k === 'binary') return 'binary frame';
  if (k === 'text') return 'text frame';
  if (k === 'batch') return `batch[${e.meta.count}]`;
  if (k === 'response') return '(response)';
  return e.meta?.label ?? 'json';
}

function valSpan(v) {
  const s = document.createElement('span');
  if (typeof v === 'string') {
    s.className = 'v-str';
    if (v.length > 400) {
      s.textContent = JSON.stringify(v.slice(0, 400)) + '… ';
      const more = el('a', '', `+${v.length - 400} chars`);
      more.href = '#';
      more.addEventListener('click', (ev) => { ev.preventDefault(); s.textContent = JSON.stringify(v); });
      s.appendChild(more);
    } else {
      s.textContent = JSON.stringify(v);
    }
  } else if (typeof v === 'number') { s.className = 'v-num'; s.textContent = String(v); }
  else if (typeof v === 'boolean') { s.className = 'v-bool'; s.textContent = String(v); }
  else { s.className = 'v-null'; s.textContent = 'null'; }
  return s;
}

function treeNode(key, val, depth) {
  if (val !== null && typeof val === 'object') {
    const entries = Array.isArray(val) ? [...val.entries()] : Object.entries(val);
    const det = document.createElement('details');
    det.open = depth < 2 || entries.length <= 4;
    const sum = document.createElement('summary');
    sum.append(el('span', 'k', key), el('span', 'hint', Array.isArray(val) ? `[${entries.length}]` : `{${entries.length}}`));
    det.append(sum);
    const box = el('div', 'children');
    for (const [k, v] of entries) box.appendChild(treeNode(String(k), v, depth + 1));
    det.append(box);
    return det;
  }
  const row = el('div', 'leaf');
  row.append(el('span', 'k', key), valSpan(val));
  return row;
}

function renderTree(parsed) {
  const box = el('div', 'tree');
  if (parsed !== null && typeof parsed === 'object') {
    const entries = Array.isArray(parsed) ? [...parsed.entries()] : Object.entries(parsed);
    for (const [k, v] of entries) box.appendChild(treeNode(String(k), v, 1));
  } else {
    box.appendChild(treeNode('value', parsed, 1));
  }
  return box;
}

function linkChip(text, seq) {
  const b = el('button', 'btn', text);
  b.addEventListener('click', () => {
    select(seq);
    tbody.querySelector(`tr[data-seq="${seq}"]`)?.scrollIntoView({ block: 'center' });
  });
  return b;
}

function renderDetail(e) {
  detail.replaceChildren();
  if (!e) { detail.hidden = true; return; }
  detail.hidden = false;

  const head = el('div', 'dhead');
  if (e.t === 'msg') {
    head.append(badge(e.meta.kind), el('b', '', e.method ?? e.meta.method ?? labelFor(e)));
  } else {
    head.append(badge(e.t === 'error' ? 'error' : e.t), el('b', '', e.t === 'nav' ? 'navigation' : 'connection event'));
  }
  const close = el('button', 'btn close', '✕');
  close.title = 'close (Esc)';
  close.addEventListener('click', deselect);
  head.append(close);
  detail.append(head);

  const g = el('div', 'meta');
  const add = (k, v) => g.append(el('span', 'k2', k), el('span', 'v2', v));
  add('time', fmtTime(e.ts));
  if (e.t !== 'nav') add('conn', `#${e.conn}`);
  if (e.t === 'msg') {
    add('direction', e.dir === 'c2s' ? 'client → server' : 'server → client');
    add('size', fmtSize(e.size) + (e.truncated ? ' (stored copy truncated)' : ''));
    if (e.meta.env) add('envelope', e.meta.env);
    if (e.meta.id !== undefined && e.meta.id !== null) add('id', String(e.meta.id));
    if (e.latency !== undefined) add('latency', fmtMs(e.latency));
    if (e.meta.errCode !== undefined || e.meta.errMsg) add('error', `${e.meta.errCode ?? ''} ${e.meta.errMsg ?? ''}`);
  } else if (e.t === 'close') {
    add('code', String(e.code));
    if (e.reason) add('reason', e.reason);
    if (e.by) add('closed by', e.by);
    if (e.ms) add('lifetime', fmtDur(e.ms));
  } else if (e.t === 'open') {
    add('url', e.path);
    if (e.upstream && e.upstream !== e.path) add('upstream', e.upstream);
  } else if (e.t === 'nav') {
    add('url', e.url);
  } else if (e.t === 'error') {
    if (e.side) add('side', e.side);
    add('message', e.message);
  }
  detail.append(g);

  if (e.t === 'msg' && (e.reqSeq !== undefined || e.resSeq !== undefined)) {
    const p = el('div', 'pair');
    if (e.reqSeq !== undefined && S.bySeq.has(e.reqSeq)) p.append(linkChip('↖ request', e.reqSeq));
    if (e.resSeq !== undefined && S.bySeq.has(e.resSeq)) {
      p.append(linkChip(`response ↘${e.latency !== undefined ? ` · ${fmtMs(e.latency)}` : ''}`, e.resSeq));
    }
    if (p.children.length) detail.append(p);
  }

  if (e.t !== 'msg') return;

  const bar = el('div', 'pbar');
  const cp = el('button', 'btn', 'copy raw');
  cp.addEventListener('click', () => {
    navigator.clipboard.writeText(e.data)
      .then(() => { cp.textContent = 'copied ✓'; setTimeout(() => (cp.textContent = 'copy raw'), 900); })
      .catch(() => {});
  });
  bar.append(cp);
  detail.append(bar);

  if (e.binary) {
    detail.append(el('div', 'note', `binary frame · ${fmtSize(e.size)} · first bytes:`), el('pre', '', e.data));
  } else if (e.truncated) {
    detail.append(
      el('div', 'note', `payload is ${fmtSize(e.size)} — exceeds the store cap, showing truncated raw text`),
      el('pre', '', e.data.slice(0, 20000) + '\n…'),
    );
  } else {
    let parsed, ok = true;
    try { parsed = JSON.parse(e.data); } catch { ok = false; }
    if (ok) detail.append(renderTree(parsed));
    else detail.append(el('pre', '', e.data));
  }
}

function select(seq) {
  S.selected = seq;
  tbody.querySelectorAll('tr.sel').forEach((r) => r.classList.remove('sel'));
  tbody.querySelector(`tr[data-seq="${seq}"]`)?.classList.add('sel');
  renderDetail(S.bySeq.get(seq));
}

function deselect() {
  S.selected = null;
  tbody.querySelectorAll('tr.sel').forEach((r) => r.classList.remove('sel'));
  renderDetail(null);
}

// ---------- toolbar ----------

let qTimer;
document.querySelector('#q').addEventListener('input', (ev) => {
  clearTimeout(qTimer);
  qTimer = setTimeout(() => { S.text = ev.target.value.trim().toLowerCase(); rebuild(); }, 120);
});

document.querySelectorAll('.chip').forEach((chip) => {
  chip.addEventListener('click', () => {
    const k = chip.dataset.kind;
    if (S.kinds.has(k)) { S.kinds.delete(k); chip.classList.remove('on'); }
    else { S.kinds.add(k); chip.classList.add('on'); }
    rebuild();
  });
});

document.querySelector('#connSel').addEventListener('change', (ev) => {
  S.conn = ev.target.value;
  rebuild();
});

pauseBtn.addEventListener('click', () => {
  S.paused = !S.paused;
  if (!S.paused) {
    S.pausedCount = 0;
    pauseBtn.textContent = '⏸ pause';
    rebuild();
  } else {
    pauseBtn.textContent = '▶ resume (0)';
  }
});

document.querySelector('#clear').addEventListener('click', () => {
  S.events = [];
  S.bySeq.clear();
  S.pending.clear();
  S.counts = { total: 0, request: 0, response: 0, notification: 0, error: 0, other: 0 };
  S.newCount = 0;
  newpill.hidden = true;
  deselect();
  updateCounts();
  rebuild();
});

document.querySelector('#export').addEventListener('click', () => {
  if (!S.events.length) return;
  const blob = new Blob([S.events.map((e) => JSON.stringify(e)).join('\n') + '\n'], { type: 'application/x-ndjson' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `wsview-${new Date().toISOString().replace(/[:.]/g, '-')}.ndjson`;
  a.click();
  URL.revokeObjectURL(a.href);
});

wrap.addEventListener('scroll', () => {
  S.follow = wrap.scrollTop + wrap.clientHeight >= wrap.scrollHeight - 6;
  if (S.follow) { S.newCount = 0; newpill.hidden = true; }
});

newpill.addEventListener('click', () => {
  wrap.scrollTop = wrap.scrollHeight;
  S.follow = true;
  S.newCount = 0;
  newpill.hidden = true;
});

window.addEventListener('keydown', (ev) => {
  if (ev.target instanceof HTMLInputElement || ev.target instanceof HTMLSelectElement) return;
  if (ev.key === 'Escape') { deselect(); return; }
  const down = ev.key === 'ArrowDown' || ev.key === 'j';
  const up = ev.key === 'ArrowUp' || ev.key === 'k';
  if (!down && !up) return;
  ev.preventDefault();
  const rows = [...tbody.querySelectorAll('tr')];
  if (!rows.length) return;
  let idx = rows.findIndex((r) => Number(r.dataset.seq) === S.selected);
  if (idx === -1) idx = down ? -1 : rows.length;
  idx = down ? Math.min(rows.length - 1, idx + 1) : Math.max(0, idx - 1);
  select(Number(rows[idx].dataset.seq));
  rows[idx].scrollIntoView({ block: 'nearest' });
});

// ---------- shared state hooks for transports ----------

function resetState() {
  S.events = [];
  S.bySeq.clear();
  S.pending.clear();
  S.conns.clear();
  S.counts = { total: 0, request: 0, response: 0, notification: 0, error: 0, other: 0 };
  S.newCount = 0;
  S.pausedCount = 0;
  newpill.hidden = true;
  deselect();
}

function setStatus(on) {
  document.querySelector('#dot').classList.toggle('on', on);
}

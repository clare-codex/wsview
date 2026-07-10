#!/usr/bin/env node
/**
 * wsview — local WebSocket reverse proxy + JSON-RPC 2.0 traffic viewer.
 * The agent connects to the proxy port; frames are forwarded verbatim to the
 * target server and a copy is mirrored to the web UI.
 */
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Duplex } from 'node:stream';
import { WebSocket, WebSocketServer, type RawData } from 'ws';

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public');

// ---------- config ----------

interface Config {
  target: string;
  port: number;
  uiPort: number;
  ring: number;      // max events kept in memory
  ringBytes: number; // max bytes of stored payloads
  storeCap: number;  // per-message stored payload cap (forwarding is never truncated)
}

function usage(): void {
  console.log(`wsview — local WebSocket / JSON-RPC 2.0 traffic viewer

usage: npm start -- --target <ws[s]://host[:port][/path]> [options]

  -t, --target <url>   upstream WebSocket server (required)
  -p, --port <n>       proxy port agents connect to   (default 9800)
  -u, --ui-port <n>    web UI port                    (default 9801)
      --ring <n>       events kept in memory          (default 5000)

If the target URL has no path, the path the agent used is forwarded
(ws://localhost:9800/rpc -> <target>/rpc); otherwise the target is used as-is.`);
}

function parseArgs(argv: string[]): Config {
  const cfg: Config = {
    target: '',
    port: 9800,
    uiPort: 9801,
    ring: 5000,
    ringBytes: 256 * 1024 * 1024,
    storeCap: 1024 * 1024,
  };
  const rest: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = (): string => argv[++i] ?? '';
    if (a === '-t' || a === '--target') cfg.target = next();
    else if (a === '-p' || a === '--port') cfg.port = Number(next());
    else if (a === '-u' || a === '--ui-port') cfg.uiPort = Number(next());
    else if (a === '--ring') cfg.ring = Number(next());
    else if (a === '-h' || a === '--help') { usage(); process.exit(0); }
    else rest.push(a);
  }
  if (!cfg.target && rest[0]) cfg.target = rest[0];
  if (!cfg.target || !/^wss?:\/\//.test(cfg.target)) { usage(); process.exit(1); }
  if (cfg.port === cfg.uiPort) { console.error('proxy port and UI port must differ'); process.exit(1); }
  return cfg;
}

const cfg = parseArgs(process.argv.slice(2));

// ---------- event log ----------

type Dir = 'c2s' | 's2c';

interface MsgMeta {
  kind: string; // request | response | notification | error | batch | json | text | binary
  method?: string;
  id?: string | number | null;
  errCode?: number;
  errMsg?: string;
  count?: number;
  env?: string;
}

// meta is normally derived in the viewer (public/viewer.js); it is attached
// here only for frames too large to store whole, parsed before truncation
type Evt =
  | { t: 'open'; seq: number; ts: number; conn: number; path: string; upstream: string }
  | { t: 'msg'; seq: number; ts: number; conn: number; dir: Dir; size: number; binary: boolean; data: string; truncated?: boolean; meta?: MsgMeta }
  | { t: 'close'; seq: number; ts: number; conn: number; code: number; reason: string; by: 'client' | 'server'; ms: number }
  | { t: 'error'; seq: number; ts: number; conn: number; side: 'client' | 'upstream'; message: string };

type NoSeq<T> = T extends unknown ? Omit<T, 'seq'> : never;

let evtSeq = 0;
let ringBytes = 0;
const ring: Evt[] = [];
const feedClients = new Set<WebSocket>();

function cost(e: Evt): number {
  return (e.t === 'msg' ? e.data.length : 0) + 200;
}

function push(e: NoSeq<Evt>): void {
  const full = { ...e, seq: ++evtSeq } as Evt;
  ring.push(full);
  ringBytes += cost(full);
  while (ring.length > cfg.ring || ringBytes > cfg.ringBytes) {
    const drop = ring.shift();
    if (!drop) break;
    ringBytes -= cost(drop);
  }
  const line = JSON.stringify(full);
  for (const c of feedClients) if (c.readyState === WebSocket.OPEN) c.send(line);
}

function rpcShape(o: Record<string, unknown>): MsgMeta | null {
  if (typeof o.method === 'string') {
    return o.id === undefined || o.id === null
      ? { kind: 'notification', method: o.method }
      : { kind: 'request', method: o.method, id: o.id as string | number };
  }
  if ('id' in o && ('result' in o || 'error' in o)) {
    if ('error' in o) {
      const err = (o.error ?? {}) as Record<string, unknown>;
      return {
        kind: 'error',
        id: o.id as string | number | null,
        errCode: typeof err.code === 'number' ? err.code : undefined,
        errMsg: err.message === undefined ? undefined : String(err.message),
      };
    }
    return { kind: 'response', id: o.id as string | number | null };
  }
  return null;
}

// keep in sync with classify() in public/viewer.js
function parseMeta(text: string): MsgMeta {
  try {
    const j: unknown = JSON.parse(text);
    if (Array.isArray(j)) return { kind: 'batch', count: j.length };
    if (j === null || typeof j !== 'object') return { kind: 'json' };
    const o = j as Record<string, unknown>;
    const direct = rpcShape(o);
    if (direct) return direct;
    for (const k of ['payload', 'data', 'message', 'body']) {
      const inner = o[k];
      if (inner && typeof inner === 'object' && !Array.isArray(inner) && (inner as Record<string, unknown>).jsonrpc === '2.0') {
        const m = rpcShape(inner as Record<string, unknown>);
        if (m) {
          for (const lk of ['type', 'event', 'action', 'kind', 'op', 'topic', 'channel']) {
            if (typeof o[lk] === 'string') { m.env = o[lk] as string; return m; }
          }
          m.env = k;
          return m;
        }
      }
    }
    return { kind: 'json' };
  } catch {
    return { kind: 'text' };
  }
}

function toBuffer(d: RawData): Buffer {
  if (Buffer.isBuffer(d)) return d;
  if (Array.isArray(d)) return Buffer.concat(d);
  return Buffer.from(d);
}

// ---------- proxy ----------

function upstreamUrl(reqPath: string): string {
  const t = new URL(cfg.target);
  if (t.pathname !== '/' || t.search) return cfg.target;
  const u = new URL(reqPath, 'http://placeholder');
  t.pathname = u.pathname;
  t.search = u.search;
  return t.toString();
}

function safeCloseCode(code: number): number {
  const ok = (code >= 1000 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006)
    || (code >= 3000 && code <= 4999);
  return ok ? code : 1000;
}

let connSeq = 0;

function wire(conn: number, client: WebSocket, upstream: WebSocket, t0: number): void {
  let closed = false;
  const record = (dir: Dir, data: RawData, isBinary: boolean): void => {
    const buf = toBuffer(data);
    if (isBinary) {
      push({ t: 'msg', ts: Date.now(), conn, dir, size: buf.length, binary: true, data: buf.subarray(0, 64).toString('hex') });
      return;
    }
    const text = buf.toString('utf8');
    const truncated = text.length > cfg.storeCap;
    push({
      t: 'msg', ts: Date.now(), conn, dir, size: buf.length, binary: false,
      data: truncated ? text.slice(0, cfg.storeCap) : text,
      ...(truncated ? { truncated: true, meta: parseMeta(text) } : {}),
    });
  };
  client.on('message', (d, isBinary) => {
    record('c2s', d, isBinary);
    if (upstream.readyState === WebSocket.OPEN) upstream.send(d, { binary: isBinary });
  });
  upstream.on('message', (d, isBinary) => {
    record('s2c', d, isBinary);
    if (client.readyState === WebSocket.OPEN) client.send(d, { binary: isBinary });
  });
  const onClose = (by: 'client' | 'server', other: WebSocket) => (code: number, reasonBuf: Buffer): void => {
    if (closed) return;
    closed = true;
    const reason = reasonBuf.toString('utf8');
    push({ t: 'close', ts: Date.now(), conn, code, reason, by, ms: Date.now() - t0 });
    if (other.readyState === WebSocket.CONNECTING) other.terminate();
    else if (other.readyState === WebSocket.OPEN) other.close(safeCloseCode(code), reason.slice(0, 120));
  };
  client.on('close', onClose('client', upstream));
  upstream.on('close', onClose('server', client));
  client.on('error', (err) => push({ t: 'error', ts: Date.now(), conn, side: 'client', message: err.message }));
  upstream.on('error', (err) => push({ t: 'error', ts: Date.now(), conn, side: 'upstream', message: err.message }));
}

const proxyHttp = http.createServer((_req, res) => {
  res.writeHead(426, { 'content-type': 'text/plain' });
  res.end(`wsview proxy — connect a WebSocket client here (forwards to ${cfg.target}); UI at http://localhost:${cfg.uiPort}\n`);
});

proxyHttp.on('upgrade', (req, socket: Duplex, head: Buffer) => {
  socket.on('error', () => {});
  const conn = ++connSeq;
  const reqPath = req.url ?? '/';
  const url = upstreamUrl(reqPath);
  const protoHeader = req.headers['sec-websocket-protocol'];
  const protocols = protoHeader ? String(protoHeader).split(',').map((s) => s.trim()).filter(Boolean) : [];
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (/^(host|connection|upgrade|sec-websocket-)/i.test(k) || v === undefined) continue;
    headers[k] = Array.isArray(v) ? v.join(', ') : v;
  }
  const t0 = Date.now();
  const upstream = new WebSocket(url, protocols, { headers });
  upstream.on('error', () => {}); // real handlers attach in wire(); this prevents pre-open crashes
  let accepted = false;
  const refuse = (why: string): void => {
    if (accepted) return;
    accepted = true;
    push({ t: 'error', ts: Date.now(), conn, side: 'upstream', message: why });
    upstream.terminate();
    if (!socket.destroyed) {
      socket.write(`HTTP/1.1 502 Bad Gateway\r\ncontent-type: text/plain\r\nconnection: close\r\n\r\nwsview: ${why}\r\n`);
      socket.destroy();
    }
  };
  upstream.on('open', () => {
    if (socket.destroyed) { upstream.terminate(); return; }
    accepted = true;
    // per-connection server so the subprotocol negotiated upstream is echoed to the client
    const wss = new WebSocketServer({
      noServer: true,
      clientTracking: false,
      handleProtocols: (ps) => (upstream.protocol && ps.has(upstream.protocol) ? upstream.protocol : false),
    });
    wss.handleUpgrade(req, socket, head, (client) => {
      push({ t: 'open', ts: Date.now(), conn, path: reqPath, upstream: url });
      wire(conn, client, upstream, t0);
    });
  });
  upstream.once('error', (err) => refuse(`cannot reach ${url} — ${err.message}`));
  upstream.on('unexpected-response', (_r, res) => refuse(`upstream replied HTTP ${res.statusCode ?? '?'} to the WebSocket handshake`));
});

// ---------- web UI ----------

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
};

const uiHttp = http.createServer(async (req, res) => {
  const reqPath = (req.url ?? '/').split('?')[0] ?? '/';
  if (reqPath === '/favicon.ico') { res.writeHead(204); res.end(); return; }
  const file = reqPath === '/' ? '/index.html' : reqPath;
  const full = path.join(PUBLIC_DIR, path.normalize(file));
  if (!full.startsWith(PUBLIC_DIR)) { res.writeHead(403); res.end(); return; }
  try {
    const body = await fs.readFile(full);
    res.writeHead(200, { 'content-type': MIME[path.extname(full)] ?? 'application/octet-stream' });
    res.end(body);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }
});

const feedWss = new WebSocketServer({ server: uiHttp, path: '/feed' });
feedWss.on('connection', (ws) => {
  feedClients.add(ws);
  ws.send(JSON.stringify({ t: 'hello', target: cfg.target, port: cfg.port, events: ring }));
  ws.on('close', () => feedClients.delete(ws));
  ws.on('error', () => feedClients.delete(ws));
});

// a viewer should survive a bad frame rather than lose its buffer
process.on('uncaughtException', (err) => console.error('[wsview] uncaught:', err));

proxyHttp.on('error', (err) => { console.error(`[wsview] proxy port ${cfg.port}: ${err.message}`); process.exit(1); });
uiHttp.on('error', (err) => { console.error(`[wsview] ui port ${cfg.uiPort}: ${err.message}`); process.exit(1); });

proxyHttp.listen(cfg.port, () => {
  uiHttp.listen(cfg.uiPort, () => {
    console.log(`
  wsview
    proxy   ws://localhost:${cfg.port}   →   ${cfg.target}
    web UI  http://localhost:${cfg.uiPort}

  Point your agent's WebSocket URL at the proxy address; frames are
  forwarded unchanged and mirrored to the UI.
`);
  });
});

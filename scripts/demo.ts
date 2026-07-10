/**
 * Demo traffic generator: starts a mock JSON-RPC 2.0 agent server on :8890
 * and an agent client that connects through the wsview proxy (:9800).
 *
 *   terminal 1: npm start -- --target ws://localhost:8890
 *   terminal 2: npm run demo
 */
import { WebSocket, WebSocketServer } from 'ws';

const UPSTREAM_PORT = 8890;
const PROXY_URL = 'ws://localhost:9800/rpc';

// ---- mock agent server (JSON-RPC 2.0 over WebSocket) ----

const wss = new WebSocketServer({ port: UPSTREAM_PORT });
wss.on('connection', (ws) => {
  const send = (obj: unknown): void => ws.send(JSON.stringify(obj));
  let srvId = 0;
  const confirmTimer = setInterval(() => {
    // server → client request: agent protocols are bidirectional
    send({ jsonrpc: '2.0', id: `srv-${++srvId}`, method: 'client/confirm', params: { question: 'continue the task?' } });
  }, 7000);
  const statusTimer = setInterval(() => {
    // enveloped JSON-RPC notification (mulerun-style)
    send({ type: 'control', payload: { jsonrpc: '2.0', method: 'session_status', params: { sessionId: '34644045-a0f2', sessionStatus: 'idle' } } });
  }, 5000);
  const telemetryTimer = setInterval(() => {
    // plain JSON frame, no JSON-RPC anywhere
    send({ type: 'telemetry', data: { cpu: 0.42, mem: 512 } });
  }, 9000);
  ws.on('close', () => { clearInterval(confirmTimer); clearInterval(statusTimer); clearInterval(telemetryTimer); });
  ws.on('message', (raw) => {
    let m: any;
    try { m = JSON.parse(String(raw)); } catch { return; } // ignore non-JSON frames
    if (m.method === 'initialize') {
      send({ jsonrpc: '2.0', id: m.id, result: { server: 'demo-agent-server', capabilities: { tools: ['search', 'run'], streaming: true } } });
    } else if (m.method === 'ping') {
      setTimeout(() => send({ jsonrpc: '2.0', id: m.id, result: { pong: true } }), 20 + Math.random() * 120);
    } else if (m.method === 'agent/run') {
      let step = 0;
      const t = setInterval(() => {
        step++;
        send({ jsonrpc: '2.0', method: 'agent/progress', params: { task: m.params?.task, step, of: 3, note: '工作中… ✓' } });
        if (step === 3) {
          clearInterval(t);
          send({ jsonrpc: '2.0', id: m.id, result: { task: m.params?.task, status: 'done', output: { summary: 'demo task finished', items: ['alpha', 'beta', 'gamma'], stats: { tokens: 1234, seconds: 0.45 } } } });
        }
      }, 150);
    } else if (m.method === 'boom') {
      send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'Method not found', data: { method: 'boom' } } });
    } else if (m.type === 'control' && m.payload?.jsonrpc === '2.0' && m.payload.method === 'session/ping') {
      // enveloped request → enveloped response
      setTimeout(() => send({ type: 'control', payload: { jsonrpc: '2.0', id: m.payload.id, result: { alive: true } } }), 15 + Math.random() * 60);
    }
    // responses to server-initiated requests are just absorbed
  });
});

// ---- agent client, connecting through the proxy ----

function startAgent(attempt: number): void {
  const ws = new WebSocket(PROXY_URL);
  let opened = false;
  ws.on('open', () => {
    opened = true;
    console.log(`[demo] agent connected via proxy ${PROXY_URL}`);
    let id = 0;
    const call = (method: string, params?: unknown): void =>
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }));
    call('initialize', { client: 'demo-agent', version: '0.1.0' });
    setInterval(() => call('ping', {}), 2500);
    setTimeout(() => call('agent/run', { task: 'summarize repo' }), 1000);
    setInterval(() => call('agent/run', { task: 'periodic sweep' }), 11000);
    setTimeout(() => call('boom', {}), 2000);
    setInterval(() => call('boom', {}), 25000);
    setInterval(() => ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'log', params: { level: 'info', msg: '心跳 ok ✓' } })), 6000);
    let envId = 0;
    setInterval(() => ws.send(JSON.stringify({ type: 'control', payload: { jsonrpc: '2.0', id: `env-${++envId}`, method: 'session/ping' } })), 4000);
  });
  ws.on('message', (raw) => {
    const m = JSON.parse(String(raw));
    if (m.method === 'client/confirm') {
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: m.id, result: { answer: 'yes' } }));
    }
  });
  ws.on('error', () => {
    if (!opened && attempt < 20) setTimeout(() => startAgent(attempt + 1), 500);
    else if (!opened) { console.error(`[demo] cannot reach proxy at ${PROXY_URL} — is wsview running?`); process.exit(1); }
  });
  ws.on('close', () => { if (opened) process.exit(0); });
}

console.log(`[demo] mock JSON-RPC server on ws://localhost:${UPSTREAM_PORT}`);
setTimeout(() => startAgent(0), 200);

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const rooms = new Map(); // code -> { tv, viewers:Set, lastFrame }
const failures = new Map(); // ip -> { count, resetAt }

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(__dirname, 'public', 'viewer.html')));
    return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 * 1024 });

function clientIp(req) {
  return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}
function tooManyFailures(ip) {
  const f = failures.get(ip);
  return f && Date.now() < f.resetAt && f.count >= 15;
}
function recordFailure(ip) {
  const now = Date.now();
  let f = failures.get(ip);
  if (!f || now > f.resetAt) f = { count: 0, resetAt: now + 60_000 };
  f.count++;
  failures.set(ip, f);
}
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function cleanup(code) {
  const r = rooms.get(code);
  if (r && !r.tv && r.viewers.size === 0) rooms.delete(code);
}

server.on('upgrade', (req, socket, head) => {
  let url;
  try { url = new URL(req.url, 'http://x'); } catch { return socket.destroy(); }
  if (url.pathname !== '/ws') return socket.destroy();
  const role = url.searchParams.get('role');
  const code = (url.searchParams.get('code') || '').toUpperCase();
  if (!/^[A-Z0-9]{6}$/.test(code) || !['tv', 'viewer'].includes(role)) return socket.destroy();
  const ip = clientIp(req);
  wss.handleUpgrade(req, socket, head, (ws) => handle(ws, role, code, ip));
});

function handle(ws, role, code, ip) {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  if (role === 'tv') {
    let r = rooms.get(code);
    if (!r) { r = { tv: null, viewers: new Set(), lastFrame: null }; rooms.set(code, r); }
    if (r.tv) { try { r.tv.close(); } catch {} }
    r.tv = ws;
    r.viewers.forEach((v) => send(v, { status: 'tv-online' }));
    ws.on('message', (data, isBinary) => {
      if (!isBinary) return;
      r.lastFrame = data;
      r.viewers.forEach((v) => {
        if (v.readyState === 1 && v.bufferedAmount < 2_000_000) v.send(data, { binary: true });
      });
    });
    ws.on('close', () => {
      if (r.tv === ws) {
        r.tv = null; r.lastFrame = null;
        r.viewers.forEach((v) => send(v, { status: 'tv-offline' }));
      }
      cleanup(code);
    });
  } else {
    if (tooManyFailures(ip)) { send(ws, { status: 'rate-limited' }); return ws.close(); }
    const r = rooms.get(code);
    if (!r || !r.tv) { recordFailure(ip); send(ws, { status: 'no-such-code' }); return ws.close(); }
    r.viewers.add(ws);
    send(ws, { status: 'tv-online' });
    if (r.lastFrame) ws.send(r.lastFrame, { binary: true });
    ws.on('close', () => { r.viewers.delete(ws); cleanup(code); });
  }
  ws.on('error', () => {});
}

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

server.listen(PORT, () => console.log('Relay listening on port ' + PORT));

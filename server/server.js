// Minecraft Classic multiplayer relay. No dependencies: it speaks WebSocket itself (RFC 6455).
// Env: PORT (default 8080), WORLD_SEED (integer, default 12345), DATA_FILE (optional, saves block edits),
//      MAX_PLAYERS (default 8), ORIGINS (optional comma list of allowed page origins, e.g. https://you.github.io).
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');

const PORT = Number(process.env.PORT) || 8080;
const SEED = Number(process.env.WORLD_SEED) || 12345;
const DATA_FILE = process.env.DATA_FILE || '';
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 8;
const ORIGINS = (process.env.ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_EDITS = 200000;
const MAX_FRAME = 64 * 1024;

// ---------- world edits (block changes made by players) ----------
const edits = new Map(); // "x,y,z" -> [typeName|null, meta]
if (DATA_FILE && fs.existsSync(DATA_FILE)) {
  try { for (const [k, v] of JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')).edits || []) edits.set(k, v); }
  catch (e) { console.error('could not read DATA_FILE', e.message); }
}
let saveTimer = null;
function scheduleSave() {
  if (!DATA_FILE || saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE, JSON.stringify({ seed: SEED, edits: [...edits] }), err => { if (err) console.error('save failed', err.message); });
  }, 2000);
}

// ---------- validation ----------
const TYPE_RE = /^[a-z0-9_]{1,32}$/;
const validInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
function cleanBlock(m) {
  if (!validInt(m.x, -3e7, 3e7) || !validInt(m.z, -3e7, 3e7) || !validInt(m.y, -24, 31)) return null;
  if (m.type !== null && (typeof m.type !== 'string' || !TYPE_RE.test(m.type))) return null;
  const meta = validInt(m.meta, 0, 255) ? m.meta : 0;
  return { x: m.x, y: m.y, z: m.z, type: m.type, meta };
}
const num = (v, lim) => (typeof v === 'number' && Number.isFinite(v) ? Math.max(-lim, Math.min(lim, v)) : 0);
function cleanPose(m) {
  return {
    x: num(m.x, 3e7), feet: num(m.feet, 300), z: num(m.z, 3e7),
    yaw: num(m.yaw, 100), pitch: num(m.pitch, 2),
    held: typeof m.held === 'string' && TYPE_RE.test(m.held) ? m.held : '',
    sit: !!m.sit,
  };
}
const cleanName = s => String(s || 'Player').replace(/[^\w \-.]/g, '').trim().slice(0, 16) || 'Player';

// ---------- players ----------
let nextId = 1;
const clients = new Map(); // id -> client

// ---------- WebSocket framing ----------
function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}
function send(c, obj) { if (!c.closed) c.socket.write(encodeFrame(0x1, Buffer.from(JSON.stringify(obj)))); }
function closeClient(c, code = 1000) {
  if (c.closed) return;
  c.closed = true;
  try { const p = Buffer.alloc(2); p.writeUInt16BE(code); c.socket.write(encodeFrame(0x8, p)); } catch (e) {}
  c.socket.end();
}
function broadcast(from, obj) {
  for (const c of clients.values()) if (c !== from) send(c, obj);
}

function onFrames(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const opcode = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_FRAME) { closeClient(c, 1009); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (!masked) { closeClient(c, 1002); return; } // clients must mask
    if (c.buf.length < need) return;
    const mask = c.buf.subarray(off, off + 4);
    const payload = Buffer.from(c.buf.subarray(off + 4, need));
    for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    c.buf = c.buf.subarray(need);
    if (opcode === 0x8) { closeClient(c); return; }
    if (opcode === 0x9) { c.socket.write(encodeFrame(0xa, payload)); continue; }
    if (opcode === 0xa) { c.alive = true; continue; }
    if (opcode === 0x1) handleMessage(c, payload.toString('utf8'));
  }
}

// ---------- game messages ----------
function handleMessage(c, text) {
  // rate limit: 80 messages per second per connection
  const now = Date.now();
  if (now - c.windowStart > 1000) { c.windowStart = now; c.count = 0; }
  if (++c.count > 80) return;
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;

  if (m.t === 'hello') {
    if (c.joined) return;
    if (clients.size >= MAX_PLAYERS) { send(c, { t: 'full' }); closeClient(c, 1000); return; }
    c.joined = true;
    c.name = cleanName(m.name);
    c.id = nextId++;
    c.pose = { x: 0, feet: 0, z: 0, yaw: 0, pitch: 0, held: '', sit: false };
    const others = [...clients.values()].filter(o => o.joined).map(o => ({ id: o.id, name: o.name, ...o.pose }));
    send(c, {
      t: 'welcome', id: c.id, seed: SEED, max: MAX_PLAYERS,
      edits: [...edits].map(([k, v]) => { const [x, y, z] = k.split(',').map(Number); return [x, y, z, v[0], v[1]]; }),
      players: others,
    });
    clients.set(c.id, c);
    broadcast(c, { t: 'join', id: c.id, name: c.name, ...c.pose });
    return;
  }
  if (!c.joined) return;

  if (m.t === 'pose') {
    c.pose = cleanPose(m);
    broadcast(c, { t: 'pose', id: c.id, ...c.pose });
    return;
  }
  if (m.t === 'block') {
    const b = cleanBlock(m);
    if (!b) return;
    if (edits.size >= MAX_EDITS && !edits.has(`${b.x},${b.y},${b.z}`)) return;
    const k = `${b.x},${b.y},${b.z}`;
    if (b.type === null) edits.set(k, [null, 0]); else edits.set(k, [b.type, b.meta]);
    scheduleSave();
    broadcast(c, { t: 'block', x: b.x, y: b.y, z: b.z, type: b.type, meta: b.meta, by: c.id });
    return;
  }
  if (m.t === 'chat') {
    const text2 = String(m.text || '').replace(/[\u0000-\u001f]/g, '').slice(0, 160);
    if (text2) { const msg = { t: 'chat', id: c.id, name: c.name, text: text2 }; send(c, msg); broadcast(c, msg); }
  }
}

// ---------- HTTP + upgrade ----------
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`Minecraft Classic relay: ${clients.size}/${MAX_PLAYERS} players online, ${edits.size} saved block edits.\n`);
});

server.on('upgrade', (req, socket) => {
  const origin = req.headers.origin || '';
  if (ORIGINS.length && !ORIGINS.includes(origin)) { socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.write('HTTP/1.1 400 Bad Request\r\n\r\n'); socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), joined: false, closed: false, alive: true, windowStart: Date.now(), count: 0 };
  socket.on('data', d => onFrames(c, d));
  socket.on('error', () => {});
  socket.on('close', () => {
    c.closed = true;
    if (c.joined && clients.get(c.id) === c) {
      clients.delete(c.id);
      broadcast(null, { t: 'leave', id: c.id });
    }
  });
});

// heartbeat: drop silent connections, keep proxies from closing idle sockets
setInterval(() => {
  for (const c of [...clients.values()]) {
    if (!c.alive) { closeClient(c, 1001); continue; }
    c.alive = false;
    try { c.socket.write(encodeFrame(0x9, Buffer.alloc(0))); } catch (e) {}
  }
}, 25000).unref();

server.listen(PORT, () => console.log(`relay listening on :${PORT}, seed ${SEED}`));

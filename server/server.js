'use strict';
/* Sunset Bay realtime server.
   - WebSocket: player positions, text chat, shared building (pieces + blocks), planted trees
   - Persistence: Postgres (Supabase etc.) via DATABASE_URL, or a local JSON file when it is not set
   Env: PORT, DATABASE_URL, ALLOWED_ORIGINS (comma list, optional), MAX_PLAYERS (default 40) */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 40;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const FILE = process.env.STATE_FILE || path.join(__dirname, 'state.json');
const LIM = { pieces: 4000, blocks: 9000, planted: 800, chat: 60 };

/* ---------- world state ---------- */
const state = { pieces: new Map(), blocks: new Map(), planted: new Map(), chat: [] };
let dirty = false;
const PIECE_TYPES = {
  foundation: [[1, 16], [1, 16]], floor: [[1, 16], [1, 16]], wall: [[1, 12], [1, 8]], door: [[2.5, 12], [2.5, 8]],
  window: [[2, 12], [2, 8]], low: [[1, 12], [.5, 2]], roof: [[1, 16], [1, 10]], gable: [[1, 12], [.5, 6]],
  pillar: [[.3, 1], [1, 8]], stairs: [[1, 6], [.5, 6]]
};
const TREE_TYPES = new Set(['tree', 'pine', 'cherry']);
const num = (v, lo, hi) => typeof v === 'number' && Number.isFinite(v) && v >= lo && v <= hi;
const int = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
const WORLD = 230;

function cleanPiece(p) {
  if (!p || typeof p !== 'object' || typeof p.id !== 'string' || !/^[\w-]{3,40}$/.test(p.id)) return null;
  const lim = PIECE_TYPES[p.type]; if (!lim) return null;
  if (!num(p.x, -WORLD, WORLD) || !num(p.z, -WORLD, WORLD) || !num(p.y, -10, 150) || !num(p.rot, -50, 50)) return null;
  if (!num(p.a, lim[0][0], lim[0][1]) || !num(p.b, lim[1][0], lim[1][1])) return null;
  if (!int(p.style, 0, 3)) return null;
  const sk = num(p.sk, 0, 4) ? p.sk : 0;
  return { id: p.id, type: p.type, x: +p.x.toFixed(3), y: +p.y.toFixed(3), z: +p.z.toFixed(3), rot: +p.rot.toFixed(4), a: p.a, b: p.b, style: p.style, sk: +sk.toFixed(2) };
}
function cleanBlock(b) {
  if (!b || !int(b.i, -WORLD, WORLD) || !int(b.j, -WORLD, WORLD) || !int(b.k, -6, 60)) return null;
  if (typeof b.t !== 'string' || !/^[a-z_]{1,16}$/.test(b.t)) return null;
  return { i: b.i, k: b.k, j: b.j, t: b.t, r: int(b.r, 0, 3) ? b.r : 0 };
}
const bkey = b => b.i + ',' + b.k + ',' + b.j;

/* ---------- persistence ---------- */
let pool = null;
async function initStore() {
  if (process.env.DATABASE_URL) {
    const { Pool } = require('pg');
    pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false }, max: 3 });
    await pool.query('CREATE TABLE IF NOT EXISTS sb_state (key text PRIMARY KEY, data jsonb NOT NULL, updated timestamptz NOT NULL DEFAULT now())');
    const r = await pool.query('SELECT key, data FROM sb_state');
    const d = {}; r.rows.forEach(x => d[x.key] = x.data);
    load(d);
    console.log('Postgres connected; loaded', counts());
  } else if (fs.existsSync(FILE)) {
    try { load(JSON.parse(fs.readFileSync(FILE, 'utf8'))); console.log('Loaded', FILE, counts()); } catch (e) { console.error('state file unreadable', e.message); }
  } else console.log('No DATABASE_URL: saving to', FILE);
}
function load(d) {
  (d.pieces || []).forEach(p => { const c = cleanPiece(p); if (c) state.pieces.set(c.id, c); });
  (d.blocks || []).forEach(b => { const c = cleanBlock(b); if (c) state.blocks.set(bkey(c), c); });
  (d.planted || []).forEach(t => { if (t && typeof t.id === 'string' && TREE_TYPES.has(t.type) && num(t.x, -WORLD, WORLD) && num(t.z, -WORLD, WORLD)) state.planted.set(t.id, { id: t.id, type: t.type, x: t.x, z: t.z, ts: +t.ts || Date.now() }); });
  state.chat = Array.isArray(d.chat) ? d.chat.slice(-LIM.chat) : [];
}
const counts = () => `${state.pieces.size} pieces, ${state.blocks.size} blocks, ${state.planted.size} trees`;
const snapshot = () => ({ pieces: [...state.pieces.values()], blocks: [...state.blocks.values()], planted: [...state.planted.values()], chat: state.chat });
let saving = false;
async function save() {
  if (!dirty || saving) return; saving = true; dirty = false;
  try {
    const s = snapshot();
    if (pool) {
      for (const k of Object.keys(s)) await pool.query('INSERT INTO sb_state (key, data, updated) VALUES ($1, $2::jsonb, now()) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated = now()', [k, JSON.stringify(s[k])]);
    } else fs.writeFileSync(FILE, JSON.stringify(s));
  } catch (e) { dirty = true; console.error('save failed', e.message); }
  saving = false;
}
setInterval(save, 6000);
['SIGTERM', 'SIGINT'].forEach(sig => process.on(sig, async () => { dirty = true; await save(); process.exit(0); }));

/* ---------- time of day (same pacing as the client) ---------- */
let tod = 17.3, lastT = Date.now();
function tickTod() {
  const now = Date.now(), dt = (now - lastT) / 1000; lastT = now;
  const sp = (tod >= 6 && tod < 19) ? 1 / 36 : ((tod >= 19 && tod < 21) || (tod >= 5 && tod < 6)) ? 1 / 20 : 1 / 10;
  tod = (tod + dt * sp) % 24;
}
setInterval(tickTod, 1000);

/* ---------- http + websocket ---------- */
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
  res.end(JSON.stringify({ ok: true, players: clients.size, world: counts() }));
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const clients = new Map(); /* id -> client */
let nextId = 1;

function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(o, except) { const m = JSON.stringify(o); for (const c of clients.values()) if (c.ws !== except && c.ws.readyState === 1) c.ws.send(m); }
const cleanName = s => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 12);
const cleanText = s => String(s || '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 160);
function cleanLook(l) {
  if (!l || typeof l !== 'object') return null;
  const o = {}; ['skin', 'hair', 'shirt', 'pants', 'pack', 'hatC'].forEach(k => { o[k] = int(l[k], 0, 40) ? l[k] : 0; });
  ['hairStyle', 'top', 'hat', 'glasses'].forEach(k => { o[k] = typeof l[k] === 'string' ? l[k].replace(/[^a-z]/g, '').slice(0, 10) : ''; });
  return o;
}
const pubPlayer = c => ({ id: c.id, name: c.name, look: c.look, x: c.x, y: c.y, z: c.z, h: c.h, sp: c.sp, area: c.area });

wss.on('connection', (ws, req) => {
  const origin = req.headers.origin;
  if (ORIGINS.length && origin && !ORIGINS.includes(origin)) { ws.close(4403, 'origin'); return; }
  if (clients.size >= MAX_PLAYERS) { send(ws, { t: 'full' }); ws.close(4429, 'full'); return; }
  const c = { ws, id: String(nextId++), name: '', look: null, x: 0, y: 5, z: 12, h: 0, sp: 0, area: 'out', joined: false, tokens: 30, chatAt: 0, lastPos: 0, alive: true, cid: '' };
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (!c.joined) {
      if (m.t !== 'hello') return;
      c.cid = String(m.cid || '').replace(/[^\w-]/g, '').slice(0, 24);
      c.name = cleanName(m.name) || 'Guest' + (1000 + Math.floor(Math.random() * 9000));
      c.look = cleanLook(m.look);
      if (num(m.x, -WORLD, WORLD) && num(m.z, -WORLD, WORLD)) { c.x = m.x; c.z = m.z; }
      /* one connection per browser: drop the older tab */
      for (const o of clients.values()) if (c.cid && o.cid === c.cid) { send(o.ws, { t: 'kicked' }); o.ws.close(4000, 'replaced'); }
      c.joined = true; clients.set(c.id, c); tickTod();
      send(ws, { t: 'init', id: c.id, name: c.name, tod, players: [...clients.values()].filter(o => o !== c).map(pubPlayer), ...snapshot() });
      broadcast({ t: 'join', p: pubPlayer(c) }, ws);
      broadcast({ t: 'count', n: clients.size });
      pushChat({ sys: true, text: c.name + ' joined' }, true);
      return;
    }
    switch (m.t) {
      case 'pos':
        if (!num(m.x, -WORLD - 20, WORLD + 20) || !num(m.z, -WORLD - 20, WORLD + 20) || !num(m.y, -20, 600)) return;
        { const now = Date.now(); if (now - c.lastPos < 60) return; c.lastPos = now; }
        c.x = m.x; c.y = m.y; c.z = m.z; c.h = num(m.h, -10, 10) ? m.h : 0; c.sp = num(m.sp, 0, 20) ? m.sp : 0;
        c.area = typeof m.area === 'string' && /^[a-z]{1,10}$/.test(m.area) ? m.area : 'out';
        broadcast({ t: 'pos', id: c.id, x: +c.x.toFixed(2), y: +c.y.toFixed(2), z: +c.z.toFixed(2), h: +c.h.toFixed(2), sp: +c.sp.toFixed(1), area: c.area }, ws);
        break;
      case 'look': {
        const l = cleanLook(m.look); if (!l) return; c.look = l;
        const nm = cleanName(m.name); if (nm) c.name = nm;
        broadcast({ t: 'look', id: c.id, look: c.look, name: c.name }, ws);
        break;
      }
      case 'chat': {
        const text = cleanText(m.text), now = Date.now();
        if (!text || now - c.chatAt < 700) return; c.chatAt = now;
        pushChat({ id: c.id, name: c.name, text });
        break;
      }
      case 'pplace': case 'prem': case 'pset': case 'bset': case 'bdel': case 'tplant':
        if (!spend(c)) { send(ws, { t: 'err', msg: 'Slow down a little' }); return; }
        build(c, m);
        break;
    }
  });
  ws.on('close', () => {
    if (!c.joined || clients.get(c.id) !== c) return;
    clients.delete(c.id);
    broadcast({ t: 'leave', id: c.id }); broadcast({ t: 'count', n: clients.size });
    pushChat({ sys: true, text: c.name + ' left' }, true);
  });
  ws.on('error', () => {});
});
function pushChat(msg, sysOnly) {
  msg.ts = Date.now();
  if (!sysOnly) { state.chat.push(msg); if (state.chat.length > LIM.chat) state.chat.shift(); dirty = true; }
  broadcast({ t: 'chat', ...msg });
}
/* token bucket: ~12 build actions a second, bursts up to 30 */
setInterval(() => { for (const c of clients.values()) c.tokens = Math.min(30, c.tokens + 6); }, 500);
function spend(c) { if (c.tokens < 1) return false; c.tokens--; return true; }

function build(c, m) {
  const ws = c.ws;
  if (m.t === 'pplace' || m.t === 'pset') {
    const p = cleanPiece(m.p); if (!p) return;
    if (m.t === 'pplace' && state.pieces.size >= LIM.pieces && !state.pieces.has(p.id)) { send(ws, { t: 'err', msg: 'The world is full of pieces' }); return; }
    if (m.t === 'pset' && !state.pieces.has(p.id)) return;
    state.pieces.set(p.id, p); dirty = true; broadcast({ t: m.t, p }, ws);
  } else if (m.t === 'prem') {
    if (typeof m.id === 'string' && state.pieces.delete(m.id)) { dirty = true; broadcast({ t: 'prem', id: m.id }, ws); }
  } else if (m.t === 'bset') {
    const b = cleanBlock(m.b); if (!b) return;
    if (state.blocks.size >= LIM.blocks && !state.blocks.has(bkey(b))) { send(ws, { t: 'err', msg: 'The world is full of blocks' }); return; }
    state.blocks.set(bkey(b), b); dirty = true; broadcast({ t: 'bset', b }, ws);
  } else if (m.t === 'bdel') {
    const b = cleanBlock({ ...m.b, t: 'x' }); if (!b) return;
    if (state.blocks.delete(bkey(b))) { dirty = true; broadcast({ t: 'bdel', b: { i: b.i, k: b.k, j: b.j } }, ws); }
  } else if (m.t === 'tplant') {
    const t = m.tree; if (!t || typeof t.id !== 'string' || !/^[\w-]{3,40}$/.test(t.id) || !TREE_TYPES.has(t.type) || !num(t.x, -WORLD, WORLD) || !num(t.z, -WORLD, WORLD)) return;
    if (state.planted.size >= LIM.planted) { send(ws, { t: 'err', msg: 'Too many planted trees' }); return; }
    const tr = { id: t.id, type: t.type, x: +t.x.toFixed(2), z: +t.z.toFixed(2), ts: Date.now() };
    state.planted.set(tr.id, tr); dirty = true; broadcast({ t: 'tplant', tree: tr }, ws);
  }
}
/* keep connections alive through proxies and drop dead ones */
setInterval(() => { for (const c of clients.values()) { if (!c.alive) { c.ws.terminate(); continue; } c.alive = false; try { c.ws.ping(); } catch (e) {} } }, 25000);

initStore().then(() => server.listen(PORT, () => console.log('Sunset Bay server listening on', PORT))).catch(e => { console.error('startup failed', e); process.exit(1); });

'use strict';
/* Sunset Bay realtime server.
   - WebSocket: player positions, text chat, shared building (pieces + blocks), planted trees
   - Persistence: Postgres (Supabase etc.) via DATABASE_URL, or a local JSON file when it is not set
   - Accounts: unique player names. Sign in with Google (set GOOGLE_CLIENT_ID) or a guest account with a recovery code
   - Passwords (scrypt) and friends with private messages (messages are not shown in the admin logs)
   - Admin panel at /admin (set ADMIN_KEY): live players, accounts, logs, kick / ban / mute
   Env: PORT, DATABASE_URL, ALLOWED_ORIGINS (comma list, optional), MAX_PLAYERS (default 40),
        ADMIN_KEY, GOOGLE_CLIENT_ID, REQUIRE_LOGIN (default on; set 0 to allow anonymous play) */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 40;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const crypto = require('crypto');
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const GOOGLE_ID = process.env.GOOGLE_CLIENT_ID || '';
const REQUIRE_LOGIN = process.env.REQUIRE_LOGIN !== '0';
const TOKENINFO = process.env.GOOGLE_TOKENINFO_URL || 'https://oauth2.googleapis.com/tokeninfo';
const FILE = process.env.STATE_FILE || path.join(__dirname, 'state.json');
const LIM = { pieces: 4000, blocks: 9000, planted: 800, chat: 60, accounts: 5000, friends: 100, requests: 30, dmPairs: 3000, dmKeep: 60 };
const LOGCAP = 400;

/* ---------- world state ---------- */
const state = { pieces: new Map(), blocks: new Map(), planted: new Map(), chat: [], accounts: new Map() };
const dms = new Map(); /* 'idA|idB' -> [{f: from id, t: time, x: text}] */
const LOGS = [];
const byName = new Map(), byTok = new Map(), bySub = new Map(); /* lookup indexes for accounts */
let dirty = false, dirtyAcc = false, dirtyLog = false, dirtyDm = false, logPend = false;
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
  (d.accounts || []).forEach(a => {
    if (!a || typeof a.id !== 'string' || typeof a.name !== 'string') return;
    state.accounts.set(a.id, { id: a.id, name: a.name, kind: a.kind === 'google' ? 'google' : 'guest', sub: String(a.sub || ''), email: String(a.email || ''), th: Array.isArray(a.th) ? a.th.slice(-5) : [], created: +a.created || Date.now(), seen: +a.seen || 0, banned: !!a.banned, muted: !!a.muted, pw: typeof a.pw === 'string' ? a.pw : '', fr: Array.isArray(a.fr) ? a.fr.slice(0, 100) : [], rq: Array.isArray(a.rq) ? a.rq.slice(0, 30) : [], rd: a.rd && typeof a.rd === 'object' ? a.rd : {} });
  });
  (d.dms || []).forEach(e => { if (Array.isArray(e) && typeof e[0] === 'string' && Array.isArray(e[1])) dms.set(e[0], e[1].slice(-LIM.dmKeep)); });
  state.accounts.forEach(a => { if (a.name) byName.set(a.name.toLowerCase(), a.id); if (a.sub) bySub.set(a.sub, a.id); a.th.forEach(h => byTok.set(h, a.id)); });
  if (Array.isArray(d.logs)) d.logs.slice(-LOGCAP).forEach(l => { if (l && typeof l.m === 'string') LOGS.push({ ts: +l.ts || Date.now(), m: l.m.slice(0, 300) }); });
}
const counts = () => `${state.pieces.size} pieces, ${state.blocks.size} blocks, ${state.planted.size} trees`;
const snapshot = () => ({ pieces: [...state.pieces.values()], blocks: [...state.blocks.values()], planted: [...state.planted.values()], chat: state.chat, accounts: [...state.accounts.values()], dms: [...dms.entries()], logs: LOGS });
let saving = false;
async function save() {
  if (saving || !(dirty || dirtyAcc || dirtyLog || dirtyDm)) return; saving = true;
  const keys = []; if (dirty) keys.push('pieces', 'blocks', 'planted', 'chat'); if (dirtyAcc) keys.push('accounts'); if (dirtyDm) keys.push('dms'); if (dirtyLog) keys.push('logs');
  const was = { d: dirty, a: dirtyAcc, l: dirtyLog, m: dirtyDm }; dirty = dirtyAcc = dirtyLog = dirtyDm = false;
  try {
    const sn = snapshot();
    if (pool) {
      for (const k of keys) await pool.query('INSERT INTO sb_state (key, data, updated) VALUES ($1, $2::jsonb, now()) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated = now()', [k, JSON.stringify(sn[k])]);
    } else fs.writeFileSync(FILE, JSON.stringify(sn));
  } catch (e) { dirty = dirty || was.d; dirtyAcc = dirtyAcc || was.a; dirtyLog = dirtyLog || was.l; dirtyDm = dirtyDm || was.m; console.error('save failed', e.message); }
  saving = false;
}
setInterval(save, 6000);
['SIGTERM', 'SIGINT'].forEach(sig => process.on(sig, async () => { dirty = dirtyAcc = dirtyLog = dirtyDm = true; await save(); process.exit(0); }));
/* logs are saved at most every 30 s so a busy server does not hammer the database */
setInterval(() => { if (logPend) { logPend = false; dirtyLog = true; } }, 30000);

/* ---------- time of day (same pacing as the client) ---------- */
let tod = 9.0, lastT = Date.now();
function tickTod() {
  const now = Date.now(), dt = (now - lastT) / 1000; lastT = now;
  const sp = (tod >= 6 && tod < 19) ? 1 / 60 : ((tod >= 19 && tod < 21) || (tod >= 5 && tod < 6)) ? 1 / 40 : 1 / 30;
  tod = (tod + dt * sp) % 24;
}
setInterval(tickTod, 1000);

/* ---------- http + websocket ---------- */
/* ---------- log ring buffer (shown in /admin, saved to the database, also printed for Render Logs) ---------- */
function log(...a) {
  const m = a.join(' ').slice(0, 300);
  LOGS.push({ ts: Date.now(), m }); if (LOGS.length > LOGCAP) LOGS.shift(); logPend = true;
  console.log(new Date().toISOString().slice(11, 19), m);
}

/* ---------- accounts ---------- */
const sha = x => crypto.createHash('sha256').update(String(x)).digest('hex');
const NAME_RE = /^[\p{L}\p{N}][\p{L}\p{M}\p{N} _.\-]{0,10}[\p{L}\p{M}\p{N}]$/u; /* 2-12 characters; letters of any language (marks allowed, never 3 in a row) */
const RESERVED = /^(admin|administrator|server|system|moderator|mod|staff|owner|sunset ?bay|guest\d*)$/i;
function checkName(raw, selfId) {
  const n = String(raw || '').normalize('NFC').replace(/\s+/g, ' ').trim();
  if (!NAME_RE.test(n) || /\p{M}{3,}/u.test(n)) return { err: 'Use 2 to 12 letters, numbers, spaces, _ . or -' };
  if (RESERVED.test(n)) return { err: 'That name is reserved' };
  const hit = byName.get(n.toLowerCase());
  if (hit && hit !== selfId) return { err: 'That name is already taken' };
  return { name: n };
}
function newAccount(kind, name, extra) {
  const a = Object.assign({ id: 'a' + crypto.randomBytes(6).toString('hex'), name: name || '', kind, sub: '', email: '', th: [], created: Date.now(), seen: Date.now(), banned: false, muted: false, pw: '', fr: [], rq: [], rd: {} }, extra || {});
  state.accounts.set(a.id, a); if (a.name) byName.set(a.name.toLowerCase(), a.id); if (a.sub) bySub.set(a.sub, a.id); dirtyAcc = true; return a;
}
function issueToken(a) {
  const t = crypto.randomBytes(18).toString('base64url'), h = sha(t);
  a.th.push(h); byTok.set(h, a.id); while (a.th.length > 5) byTok.delete(a.th.shift());
  dirtyAcc = true; return t;
}
const accByToken = t => { const id = byTok.get(sha(String(t || '').slice(0, 64))); return id ? state.accounts.get(id) : null; };
/* ---------- passwords: scrypt, 6-64 characters ---------- */
const scrypt = (pw, salt) => new Promise((res, rej) => crypto.scrypt(String(pw), salt, 32, (e, k) => e ? rej(e) : res(k)));
async function hashPw(pw) { const salt = crypto.randomBytes(16); const k = await scrypt(pw, salt); return 's1$' + salt.toString('hex') + '$' + k.toString('hex'); }
async function checkPw(pw, stored) {
  const p = String(stored || '').split('$');
  if (p[0] !== 's1' || p.length !== 3) { await scrypt(pw, Buffer.alloc(16)); return false; } /* same work whether or not the account exists */
  const k = await scrypt(pw, Buffer.from(p[1], 'hex')), want = Buffer.from(p[2], 'hex');
  return want.length === k.length && crypto.timingSafeEqual(k, want);
}
const WEAK = new Set(['123456', '1234567', '12345678', '123456789', 'password', 'qwerty', 'abcdef', 'abc123', '111111', '000000', 'iloveyou']);
function pwProblem(pw, name) {
  pw = String(pw || '');
  if (pw.length < 6) return 'Password needs at least 6 characters';
  if (pw.length > 64) return 'Password is too long (64 characters at most)';
  if (WEAK.has(pw.toLowerCase())) return 'That password is too easy to guess';
  if (name && pw.toLowerCase() === String(name).toLowerCase()) return 'Password cannot be the same as your name';
  return '';
}
function setName(a, name) { if (a.name) byName.delete(a.name.toLowerCase()); a.name = name; byName.set(name.toLowerCase(), a.id); dirtyAcc = true; }
function dropAccount(a) {
  kickAccount(a.id, 'deleted');
  state.accounts.forEach(x => { x.fr = x.fr.filter(i => i !== a.id); x.rq = x.rq.filter(i => i !== a.id); delete x.rd[a.id]; dms.delete(pairKey(a.id, x.id)); });
  dirtyDm = true;
  if (a.name) byName.delete(a.name.toLowerCase()); if (a.sub) bySub.delete(a.sub); a.th.forEach(h => byTok.delete(h));
  state.accounts.delete(a.id); dirtyAcc = true;
}
/* disconnect every live connection of an account (and its short-lived ghost) */
function kickAccount(aid, why) {
  for (const c of [...clients.values()]) if (c.acc && c.acc.id === aid) { send(c.ws, { t: 'kicked', why }); try { c.ws.close(4003, why); } catch (e) {} }
  const g = ghosts.get(aid); if (g) { clearTimeout(g.timer); ghosts.delete(aid); finalLeave(g); }
}
async function verifyGoogle(cred) {
  if (!GOOGLE_ID) throw new Error('Google sign-in is not set up on this server');
  const r = await fetch(TOKENINFO + '?id_token=' + encodeURIComponent(String(cred || '').slice(0, 4096)), { signal: AbortSignal.timeout(8000) });
  if (!r.ok) throw new Error('Google did not accept that sign-in');
  const t = await r.json();
  if (t.aud !== GOOGLE_ID) throw new Error('This sign-in was made for a different app');
  if (!/^(https:\/\/)?accounts\.google\.com$/.test(String(t.iss || ''))) throw new Error('Not a Google sign-in');
  if (!(+t.exp * 1000 > Date.now())) throw new Error('That sign-in has expired. Try again');
  if (!t.sub) throw new Error('Google sign-in had no account id');
  return { sub: String(t.sub), email: (t.email_verified === 'true' || t.email_verified === true) ? String(t.email || '').slice(0, 80) : '', given: String(t.given_name || '') };
}

/* ---------- tiny rate limiter + http helpers ---------- */
const hits = new Map();
function limited(key, max, ms) {
  const now = Date.now(), arr = (hits.get(key) || []).filter(t => now - t < ms);
  if (arr.length >= max) { hits.set(key, arr); return true; }
  arr.push(now); hits.set(key, arr); return false;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.length || now - v[v.length - 1] > 3600000) hits.delete(k); }, 600000);
const ipOf = req => String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '';
const maskIp = ip => ip.includes(':') ? ip.split(':').slice(0, 4).join(':') + '::' : ip.replace(/\.\d+$/, '.x');
function json(res, code, o) { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(o)); }
function cors(req, res) {
  const o = req.headers.origin;
  if (ORIGINS.length) { if (o && ORIGINS.includes(o)) { res.setHeader('access-control-allow-origin', o); res.setHeader('vary', 'origin'); } }
  else res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-headers', 'content-type'); res.setHeader('access-control-allow-methods', 'GET,POST,OPTIONS');
}
function readBody(req, res, cb) {
  let n = 0; const parts = [];
  req.on('data', ch => { n += ch.length; if (n > 8192) { req.destroy(); return; } parts.push(ch); });
  req.on('end', () => { let b = {}; try { b = JSON.parse(Buffer.concat(parts).toString('utf8') || '{}'); } catch (e) {} if (!b || typeof b !== 'object') b = {}; try { cb(b); } catch (e) { console.error('http error', e && e.stack || e); json(res, 500, { ok: false, err: 'Server error' }); } });
}
async function apiLogin(req, res, b) {
  const ip = ipOf(req), fail = (err, extra) => json(res, 200, Object.assign({ ok: false, err }, extra || {}));
  if (limited('login:' + ip, 60, 60000)) return json(res, 429, { ok: false, err: 'Too many tries. Wait a minute.' });
  if (b.kind === 'guest') {
    if (state.accounts.size >= LIM.accounts) return fail('The server has no room for new accounts');
    if (limited('new:' + ip, 8, 3600000)) return fail('Too many new accounts from your network. Try again later.');
    const r = checkName(b.name); if (r.err) return fail(r.err);
    const a = newAccount('guest', r.name), tok = issueToken(a);
    log('account created:', a.name, '(guest)', maskIp(ip));
    return json(res, 200, { ok: true, token: tok, name: a.name, kind: 'guest', created: true, hasPw: false });
  }
  if (b.kind === 'signup') {
    if (state.accounts.size >= LIM.accounts) return fail('The server has no room for new accounts');
    if (limited('new:' + ip, 8, 3600000)) return fail('Too many new accounts from your network. Try again later.');
    const r = checkName(b.name); if (r.err) return fail(r.err);
    const pp = pwProblem(b.password, r.name); if (pp) return fail(pp);
    const hash = await hashPw(b.password);
    const again = checkName(r.name); if (again.err) return fail(again.err); /* someone may have taken it while we hashed */
    const a = newAccount('guest', r.name, { pw: hash }), tok = issueToken(a);
    log('account created:', a.name, '(password)', maskIp(ip));
    return json(res, 200, { ok: true, token: tok, name: a.name, kind: 'guest', created: true, hasPw: true });
  }
  if (b.kind === 'password') {
    const lc = String(b.name || '').normalize('NFC').replace(/\s+/g, ' ').trim().toLowerCase().slice(0, 40);
    if (limited('pw:' + ip + ':' + lc, 10, 600000)) return fail('Too many tries for this name. Wait 10 minutes.');
    const a = state.accounts.get(byName.get(lc));
    const good = await checkPw(String(b.password || '').slice(0, 200), a && a.pw);
    if (!a || !a.pw || !good) { log('sign-in failed:', lc.slice(0, 20), maskIp(ip)); return fail('Wrong name or password'); }
    if (a.banned) return fail('This account is banned', { banned: true });
    a.seen = Date.now(); const tok = issueToken(a); log('sign-in:', a.name, '(password)', maskIp(ip));
    return json(res, 200, { ok: true, token: tok, name: a.name, kind: a.kind, hasPw: true });
  }
  if (b.kind === 'setpw') {
    const a = accByToken(b.token); if (!a) return fail('Sign in again', { bad: true });
    if (!a.name) return fail('Choose your name first');
    if (limited('setpw:' + a.id, 8, 600000)) return fail('Too many tries. Wait 10 minutes.');
    if (a.pw && !(await checkPw(String(b.old || '').slice(0, 200), a.pw))) return fail('Your current password is not right');
    const pp = pwProblem(b.password, a.name); if (pp) return fail(pp);
    a.pw = await hashPw(b.password);
    a.th.forEach(h => byTok.delete(h)); a.th = []; /* other devices have to sign in again */
    const tok = issueToken(a); dirtyAcc = true; log('password set:', a.name, maskIp(ip));
    return json(res, 200, { ok: true, token: tok, name: a.name, kind: a.kind, hasPw: true });
  }
  if (b.kind === 'resume') {
    const a = accByToken(b.token);
    if (!a) return fail('That code is not valid', { bad: true });
    if (a.banned) return fail('This account is banned', { banned: true });
    a.seen = Date.now(); log('sign-in:', a.name || '(no name)', '(code)', maskIp(ip));
    return json(res, 200, { ok: true, token: String(b.token).slice(0, 64), name: a.name, kind: a.kind, needName: !a.name, hasPw: !!a.pw });
  }
  if (b.kind === 'setname') {
    const a = accByToken(b.token); if (!a) return fail('Sign in again', { bad: true });
    if (a.name) return fail('Your name is already set');
    const r = checkName(b.name, a.id); if (r.err) return fail(r.err);
    setName(a, r.name); log('name chosen:', a.name, '(' + a.kind + ')');
    return json(res, 200, { ok: true, name: a.name });
  }
  if (b.kind === 'google') {
    verifyGoogle(b.credential).then(g => {
      let a = state.accounts.get(bySub.get(g.sub));
      if (!a) {
        if (state.accounts.size >= LIM.accounts) return fail('The server has no room for new accounts');
        a = newAccount('google', '', { sub: g.sub, email: g.email }); log('account created: (Google)', g.email || g.sub.slice(0, 6), maskIp(ip));
      }
      if (a.banned) return fail('This account is banned', { banned: true });
      if (g.email && a.email !== g.email) { a.email = g.email; dirtyAcc = true; }
      a.seen = Date.now(); const tok = issueToken(a); log('sign-in:', a.name || '(no name)', '(Google)', maskIp(ip));
      const sg = checkName(g.given.slice(0, 12), a.id);
      json(res, 200, { ok: true, token: tok, name: a.name, kind: 'google', needName: !a.name, hasPw: !!a.pw, suggest: sg.name || '' });
    }).catch(e => { log('google sign-in failed:', String(e.message).slice(0, 80), maskIp(ip)); fail(e.message || 'Google sign-in failed'); });
    return;
  }
  fail('Unknown request');
}
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/admin' || url.startsWith('/admin/')) return adminHttp(req, res, url);
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  if (url === '/api/config') return json(res, 200, { ok: true, google: GOOGLE_ID || null, requireLogin: REQUIRE_LOGIN, players: online() });
  if (url === '/api/login' && req.method === 'POST') return readBody(req, res, b => { apiLogin(req, res, b).catch(e => { console.error('login error', e && e.stack || e); if (!res.headersSent) json(res, 500, { ok: false, err: 'Server error' }); }); });
  json(res, 200, { ok: true, players: online(), world: counts() });
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
/* ---------- friends and private messages ---------- */
const pairKey = (a, b) => a < b ? a + '|' + b : b + '|' + a;
const accOnline = id => [...clients.values()].filter(c => c.acc && c.acc.id === id);
const unreadOf = (a, fid) => { const rd = a.rd[fid] || 0; return (dms.get(pairKey(a.id, fid)) || []).filter(m => m.f === fid && m.t > rd).length; };
function friendList(a) {
  const get = id => state.accounts.get(id);
  return {
    friends: a.fr.map(get).filter(Boolean).map(f => ({ id: f.id, name: f.name, on: accOnline(f.id).length ? 1 : 0, un: unreadOf(a, f.id) })),
    req: a.rq.map(get).filter(Boolean).map(f => ({ id: f.id, name: f.name }))
  };
}
const toAcc = (a, msg) => accOnline(a.id).forEach(c => send(c.ws, msg));
const pushFriends = a => toAcc(a, Object.assign({ t: 'fr_list' }, friendList(a)));
const presence = (a, on) => a.fr.forEach(fid => toAcc({ id: fid }, { t: 'fr_on', id: a.id, on }));
function makeFriends(a, b) {
  a.rq = a.rq.filter(i => i !== b.id); b.rq = b.rq.filter(i => i !== a.id);
  if (!a.fr.includes(b.id)) a.fr.push(b.id); if (!b.fr.includes(a.id)) b.fr.push(a.id);
  dirtyAcc = true; pushFriends(a); pushFriends(b);
  toAcc(a, { t: 'info', msg: 'You and ' + b.name + ' are now friends' }); toAcc(b, { t: 'info', msg: 'You and ' + a.name + ' are now friends' });
  log('friends:', a.name, '+', b.name);
}
function unfriend(a, b) {
  a.fr = a.fr.filter(i => i !== b.id); b.fr = b.fr.filter(i => i !== a.id); delete a.rd[b.id]; delete b.rd[a.id];
  dms.delete(pairKey(a.id, b.id)); dirtyAcc = dirtyDm = true; pushFriends(a); pushFriends(b);
}
function friendMsg(c, m, ws) {
  const me = c.acc, err = msg => send(ws, { t: 'err', msg });
  const other = state.accounts.get(String(m.id || m.to || m.with || ''));
  switch (m.t) {
    case 'fr_add': {
      if (limited('fr:' + me.id, 12, 60000)) return err('Slow down a little');
      const lc = String(m.name || '').normalize('NFC').replace(/\s+/g, ' ').trim().toLowerCase();
      const t = state.accounts.get(byName.get(lc));
      if (!t || t.banned) return err('No player with that name');
      if (t.id === me.id) return err('That is you');
      if (me.fr.includes(t.id)) return err('You are already friends');
      if (me.rq.includes(t.id)) return makeFriends(me, t); /* they had asked me first */
      if (t.rq.includes(me.id)) return err('Request already sent');
      if (me.fr.length >= LIM.friends || t.fr.length >= LIM.friends) return err('Friend list is full');
      if (t.rq.length >= LIM.requests) return err('That player has too many requests waiting');
      t.rq.push(me.id); dirtyAcc = true; pushFriends(t);
      toAcc(t, { t: 'info', msg: me.name + ' sent you a friend request' });
      send(ws, { t: 'info', msg: 'Request sent to ' + t.name }); log('friend request:', me.name, '->', t.name);
      return;
    }
    case 'fr_acc': if (other && me.rq.includes(other.id)) makeFriends(me, other); return;
    case 'fr_dec': if (other && me.rq.includes(other.id)) { me.rq = me.rq.filter(i => i !== other.id); dirtyAcc = true; pushFriends(me); } return;
    case 'fr_del': if (other && me.fr.includes(other.id)) { unfriend(me, other); log('unfriended:', me.name, '-', other.name); } return;
    case 'dm': {
      if (!other || !me.fr.includes(other.id)) return err('You can only message friends');
      if (me.muted) return err('You are muted');
      const text = cleanText(m.text).slice(0, 200), now = Date.now();
      if (!text || now - (c.dmAt || 0) < 500) return; c.dmAt = now;
      const key = pairKey(me.id, other.id);
      if (!dms.has(key) && dms.size >= LIM.dmPairs) return err('Message storage is full');
      const arr = dms.get(key) || []; arr.push({ f: me.id, t: now, x: text }); while (arr.length > LIM.dmKeep) arr.shift(); dms.set(key, arr); dirtyDm = true;
      toAcc(other, { t: 'dm', with: me.id, name: me.name, mine: 0, x: text, ts: now });
      toAcc(me, { t: 'dm', with: other.id, name: other.name, mine: 1, x: text, ts: now });
      return;
    }
    case 'dm_hist': {
      if (!other || !me.fr.includes(other.id)) return;
      const arr = dms.get(pairKey(me.id, other.id)) || [];
      me.rd[other.id] = Date.now(); dirtyAcc = true;
      send(ws, { t: 'dm_hist', with: other.id, msgs: arr.slice(-40).map(x => ({ mine: x.f === me.id ? 1 : 0, x: x.x, ts: x.t })) });
      pushFriends(me); return;
    }
    case 'dm_read': if (other && me.fr.includes(other.id)) { me.rd[other.id] = Date.now(); dirtyAcc = true; } return;
  }
}
const clients = new Map(); /* id -> live client */
const ghosts = new Map();  /* cid -> client whose socket dropped; kept briefly so a quick reconnect is invisible */
const GRACE_MS = 12000;
const online = () => clients.size + ghosts.size;
let nextId = 1;
process.on('uncaughtException', e => console.error('uncaught', e && e.stack || e));
process.on('unhandledRejection', e => console.error('unhandled', e && e.stack || e));

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
  const c = { ws, id: '', name: '', look: null, x: 0, y: 5, z: 12, h: 0, sp: 0, area: 'out', joined: false, tokens: 30, chatAt: 0, lastPos: 0, missed: 0, cid: '', replaced: false, timer: null, acc: null, ipm: maskIp(ipOf(req)), since: 0, renameAt: 0 };
  ws.on('pong', () => { c.missed = 0; });
  ws.on('message', raw => {
    try { onMessage(c, ws, raw); } catch (e) { console.error('message error', e && e.stack || e); }
  });
  ws.on('close', code => {
    if (!c.joined || c.replaced || clients.get(c.id) !== c) return;
    clients.delete(c.id);
    log('drop', c.name, 'id=' + c.id, 'code=' + code, 'online=' + online());
    if (c.cid && code !== 4003) {
      /* keep the avatar for a moment: a quick reconnect (phone sleep, network blip) should be invisible */
      const old = ghosts.get(c.cid); if (old && old !== c) { clearTimeout(old.timer); finalLeave(old); }
      ghosts.set(c.cid, c);
      c.timer = setTimeout(() => { if (ghosts.get(c.cid) === c) ghosts.delete(c.cid); finalLeave(c); }, GRACE_MS);
    } else finalLeave(c);
  });
  ws.on('error', () => {});
});
function finalLeave(c) {
  if (c.acc && !accOnline(c.acc.id).length) presence(c.acc, 0);
  broadcast({ t: 'leave', id: c.id }); broadcast({ t: 'count', n: online() });
  pushChat({ sys: true, text: c.name + ' left' }, true);
  log('left', c.name, 'id=' + c.id, 'online=' + online());
}
function onMessage(c, ws, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  if (!c.joined) {
    if (m.t !== 'hello') return;
    const acc = m.tok ? accByToken(m.tok) : null;
    if (REQUIRE_LOGIN && (!acc || !acc.name)) { send(ws, { t: 'auth' }); ws.close(4401, 'auth'); return; }
    if (acc && acc.banned) { send(ws, { t: 'banned' }); ws.close(4003, 'banned'); log('banned account refused:', acc.name, c.ipm); return; }
    c.acc = acc && acc.name ? acc : null;
    c.cid = c.acc ? c.acc.id : String(m.cid || '').replace(/[^\w-]/g, '').slice(0, 24);
    c.look = cleanLook(m.look);
    if (num(m.x, -WORLD, WORLD) && num(m.z, -WORLD, WORLD)) { c.x = m.x; c.z = m.z; }
    /* same browser reconnecting: take over its previous identity so nobody sees a leave + join */
    let prev = null;
    if (c.cid) {
      for (const o of clients.values()) if (o.cid === c.cid) { prev = o; break; }
      if (prev) { prev.replaced = true; clients.delete(prev.id); send(prev.ws, { t: 'kicked' }); try { prev.ws.close(4000, 'replaced'); } catch (e) {} }
      else if (ghosts.has(c.cid)) { prev = ghosts.get(c.cid); clearTimeout(prev.timer); ghosts.delete(c.cid); }
    }
    c.name = c.acc ? c.acc.name : (cleanName(m.name) || (prev && prev.name) || 'Guest' + (1000 + Math.floor(Math.random() * 9000)));
    c.since = Date.now(); if (c.acc && Date.now() - c.acc.seen > 60000) { c.acc.seen = Date.now(); dirtyAcc = true; }
    if (!prev && online() >= MAX_PLAYERS) { send(ws, { t: 'full' }); ws.close(4429, 'full'); return; }
    c.id = prev ? prev.id : String(nextId++);
    c.joined = true; clients.set(c.id, c); tickTod();
    send(ws, { t: 'init', id: c.id, name: c.name, acc: c.acc ? 1 : 0, pw: c.acc && c.acc.pw ? 1 : 0, tod, players: [...clients.values(), ...ghosts.values()].filter(o => o !== c).map(pubPlayer), ...snapshot() });
    if (c.acc) { send(ws, Object.assign({ t: 'fr_list' }, friendList(c.acc))); presence(c.acc, 1); }
    if (prev) {
      broadcast({ t: 'look', id: c.id, look: c.look, name: c.name }, ws);
      broadcast({ t: 'pos', id: c.id, x: +c.x.toFixed(2), y: +c.y.toFixed(2), z: +c.z.toFixed(2), h: 0, sp: 0, area: 'out' }, ws);
      log('back', c.name, 'id=' + c.id, 'online=' + online());
    } else {
      broadcast({ t: 'join', p: pubPlayer(c) }, ws);
      broadcast({ t: 'count', n: online() });
      pushChat({ sys: true, text: c.name + ' joined' }, true);
      log('join', c.name, 'id=' + c.id, 'online=' + online());
    }
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
      if (!c.acc) { const nm = cleanName(m.name); if (nm) c.name = nm; }
      broadcast({ t: 'look', id: c.id, look: c.look, name: c.name }, ws);
      break;
    }
    case 'chat': {
      const text = cleanText(m.text), now = Date.now();
      if (!text || now - c.chatAt < 700) return; c.chatAt = now;
      if (c.acc && c.acc.muted) { send(ws, { t: 'err', msg: 'You are muted' }); return; }
      log('chat', c.name + ':', text);
      pushChat({ id: c.id, name: c.name, text });
      break;
    }
    case 'ping': send(ws, { t: 'pong' }); break;
    case 'fr_add': case 'fr_acc': case 'fr_dec': case 'fr_del': case 'dm': case 'dm_hist': case 'dm_read':
      if (c.acc) friendMsg(c, m, ws);
      break;
    case 'rename': {
      if (!c.acc) return;
      const now = Date.now(); if (now - c.renameAt < 30000) { send(ws, { t: 'err', msg: 'You can change your name every 30 seconds' }); return; }
      const r = checkName(m.name, c.acc.id); if (r.err) { send(ws, { t: 'err', msg: r.err }); return; }
      if (r.name === c.acc.name) return;
      c.renameAt = now; const old = c.acc.name; setName(c.acc, r.name); c.name = r.name; log('rename:', old, '->', r.name);
      send(ws, { t: 'name', name: r.name }); broadcast({ t: 'look', id: c.id, look: c.look, name: c.name }, ws);
      break;
    }
    case 'pplace': case 'prem': case 'pset': case 'bset': case 'bdel': case 'tplant':
      if (!spend(c)) { send(ws, { t: 'err', msg: 'Slow down a little' }); return; }
      build(c, m);
      break;
  }
}
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
/* keep connections alive through proxies, drop dead ones after two missed pings */
setInterval(() => { for (const c of clients.values()) { if (c.missed >= 2) { c.ws.terminate(); continue; } c.missed++; try { c.ws.ping(); } catch (e) {} } }, 25000);
/* clock + heartbeat: every client gets this every 10 s, so a silent connection is detected quickly */
setInterval(() => { tickTod(); broadcast({ t: 'time', tod, now: Date.now() }); }, 10000);

/* ---------- admin panel: /admin (needs ADMIN_KEY) ---------- */
const ADMIN_HTML = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Sunset Bay Admin</title>\n<style>\n:root{--bg:#1d1429;--panel:#2a1d3b;--line:#46345e;--ink:#fff1e3;--dim:#b9a6cf;--peach:#ffb48a;--ok:#9be3c1;--bad:#ff7a8a;--warn:#ffd27a}\n*{box-sizing:border-box}\n[hidden]{display:none!important}\nbody{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,\"Segoe UI\",sans-serif}\nheader{display:flex;flex-wrap:wrap;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--panel)}\nh1{font-size:17px;margin:0;margin-right:auto}h1 span{color:var(--peach)}\n.pill{padding:3px 10px;border-radius:99px;background:rgba(255,255,255,.08);color:var(--dim);font-size:12px;font-weight:700}\n.pill b{color:var(--ink)}\nmain{padding:14px 16px;max-width:1100px;margin:0 auto}\nnav{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}\nbutton,input,select{font:inherit;color:var(--ink);background:rgba(255,255,255,.08);border:1px solid var(--line);border-radius:8px;padding:6px 12px}\nbutton{cursor:pointer;font-weight:700}button:hover{background:rgba(255,255,255,.16)}\nbutton.on{background:var(--peach);color:#2a1b3d;border-color:var(--peach)}\nbutton.bad{border-color:var(--bad);color:var(--bad)}button.ok{border-color:var(--ok);color:var(--ok)}\nbutton.sm{padding:2px 9px;font-size:12px}\ntable{width:100%;border-collapse:collapse}th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:middle}\nth{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.08em}\n.tag{display:inline-block;padding:1px 8px;border-radius:99px;font-size:11px;font-weight:800;background:rgba(255,255,255,.1);color:var(--dim)}\n.tag.g{background:#3a5a8a;color:#dbe8ff}.tag.b{background:#5a2a3a;color:var(--bad)}.tag.m{background:#5a4a2a;color:var(--warn)}.tag.on{background:#25503f;color:var(--ok)}\n.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px}\n#logs{font:12px/1.5 ui-monospace,Consolas,monospace;background:rgba(0,0,0,.3);border:1px solid var(--line);border-radius:10px;padding:10px;max-height:62vh;overflow:auto;white-space:pre-wrap;word-break:break-word}\n#logs div.chat{color:#ffd9bf}#logs div.admin{color:var(--warn)}#logs div.warn{color:var(--bad)}\n#login{max-width:340px;margin:12vh auto;background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:20px;display:flex;flex-direction:column;gap:10px}\n#login p{margin:0;color:var(--dim)}\n.err{color:var(--bad);min-height:1.3em}\n.empty{color:var(--dim);padding:14px 0}\n</style></head><body>\n<div id=\"login\" hidden>\n  <h1>Sunset Bay <span>Admin</span></h1>\n  <p>Enter the admin key you set on the server.</p>\n  <input id=\"key\" type=\"password\" placeholder=\"Admin key\" autocomplete=\"current-password\">\n  <button id=\"go\" class=\"on\">Open</button>\n  <div class=\"err\" id=\"lerr\"></div>\n</div>\n<div id=\"app\" hidden>\n<header>\n  <h1>Sunset Bay <span>Admin</span></h1>\n  <span class=\"pill\">Online <b id=\"sOnline\">0</b></span>\n  <span class=\"pill\">Accounts <b id=\"sAcc\">0</b></span>\n  <span class=\"pill\">Game time <b id=\"sTod\">--</b></span>\n  <span class=\"pill\">Up <b id=\"sUp\">--</b></span>\n  <span class=\"pill\" id=\"sWorld\"></span>\n  <button class=\"sm\" id=\"out\">Sign out</button>\n</header>\n<main>\n  <nav><button data-t=\"players\" class=\"on\">Players</button><button data-t=\"accounts\">Accounts</button><button data-t=\"logs\">Logs</button><button data-t=\"tools\">Tools</button></nav>\n  <section id=\"t-players\"></section>\n  <section id=\"t-accounts\" hidden></section>\n  <section id=\"t-logs\" hidden>\n    <div class=\"row\"><input id=\"lf\" placeholder=\"Filter logs (name, word)\u2026\" style=\"flex:1;min-width:160px\"><label><input type=\"checkbox\" id=\"lchat\" checked> chat</label><button class=\"sm\" id=\"ldl\">Download</button></div>\n    <div id=\"logs\"></div>\n  </section>\n  <section id=\"t-tools\" hidden>\n    <div class=\"row\"><input id=\"say\" placeholder=\"Announcement to all players\" maxlength=\"140\" style=\"flex:1;min-width:200px\"><button id=\"saygo\" class=\"on\">Send</button></div>\n    <p class=\"empty\">Announcements appear in everyone's chat as [Admin]. Kick, ban and mute are on the Players and Accounts tabs.</p>\n  </section>\n</main></div>\n<script>\n(function(){\nvar KEY='';try{KEY=sessionStorage.getItem('sbk')||''}catch(e){}\nvar DATA=null,TAB='players',timer=null;\nfunction $(i){return document.getElementById(i)}\nfunction el(tag,cls,txt){var e=document.createElement(tag);if(cls)e.className=cls;if(txt!=null)e.textContent=txt;return e}\nfunction ago(ts){if(!ts)return 'never';var s=Math.max(0,(Date.now()-ts)/1000|0);if(s<60)return s+'s ago';if(s<3600)return (s/60|0)+'m ago';if(s<86400)return (s/3600|0)+'h ago';return (s/86400|0)+'d ago'}\nfunction api(path,body){\n  var o={headers:{'x-admin-key':KEY,'content-type':'application/json'}};\n  if(body){o.method='POST';o.body=JSON.stringify(body)}\n  return fetch(path,o).then(function(r){return r.json().then(function(j){j.status=r.status;return j})});\n}\nfunction showLogin(msg){$('app').hidden=true;$('login').hidden=false;$('lerr').textContent=msg||'';clearInterval(timer)}\nfunction start(){$('login').hidden=true;$('app').hidden=false;poll();clearInterval(timer);timer=setInterval(poll,3000)}\nfunction poll(){\n  if(document.hidden)return;\n  api('/admin/api/state').then(function(d){\n    if(d.status===401){showLogin('Wrong key');return}\n    if(d.status===429){showLogin('Too many wrong tries. Wait 10 minutes.');return}\n    if(!d.ok)return;DATA=d;render();\n  }).catch(function(){});\n}\nfunction act(o,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;api('/admin/api/act',o).then(function(r){if(!r.ok)alert(r.err||'Failed');poll()})}\nfunction render(){\n  var d=DATA;$('sOnline').textContent=d.online;$('sAcc').textContent=d.accountTotal;\n  var h=Math.floor(d.tod),m=Math.floor((d.tod-h)*60);$('sTod').textContent=(h<10?'0':'')+h+':'+(m<10?'0':'')+m;\n  $('sUp').textContent=d.up>3600?(d.up/3600).toFixed(1)+'h':Math.floor(d.up/60)+'m';\n  $('sWorld').textContent=d.world+(d.google?' \u00b7 Google on':' \u00b7 Google off')+(d.requireLogin?'':' \u00b7 login optional');\n  if(TAB==='players')renderPlayers();if(TAB==='accounts')renderAccounts();if(TAB==='logs')renderLogs();\n}\nfunction table(heads,rows,emptyTxt){\n  if(!rows.length)return el('div','empty',emptyTxt);\n  var t=el('table'),hr=el('tr');heads.forEach(function(x){hr.appendChild(el('th',null,x))});t.appendChild(hr);\n  rows.forEach(function(r){t.appendChild(r)});return t;\n}\nfunction td(x){var c=el('td');if(x&&x.nodeType)c.appendChild(x);else c.textContent=x==null?'':x;return c}\nfunction tag(txt,cls){return el('span','tag '+(cls||''),txt)}\nfunction btn(txt,cls,fn){var b=el('button','sm '+(cls||''),txt);b.onclick=fn;return b}\nfunction renderPlayers(){\n  var host=$('t-players');host.textContent='';\n  var rows=DATA.players.map(function(p){\n    var r=el('tr');\n    var name=el('span');name.appendChild(document.createTextNode(p.name+' '));if(p.kind==='google')name.appendChild(tag('Google','g'));else if(p.kind==='guest')name.appendChild(tag('Guest'));else name.appendChild(tag('No account'));if(p.muted)name.appendChild(tag('muted','m'));\n    r.appendChild(td(name));r.appendChild(td(p.area+' ('+p.x+', '+p.z+')'));r.appendChild(td(ago(p.since)));r.appendChild(td(p.ip));\n    var a=el('span');\n    a.appendChild(btn('Kick','',function(){act({act:'kick',id:p.id},'Kick '+p.name+'?')}));a.appendChild(document.createTextNode(' '));\n    if(p.aid){a.appendChild(btn(p.muted?'Unmute':'Mute','',function(){act({act:p.muted?'unmute':'mute',aid:p.aid})}));a.appendChild(document.createTextNode(' '));a.appendChild(btn('Ban','bad',function(){act({act:'ban',aid:p.aid},'Ban '+p.name+'? They cannot sign in again until unbanned.')}))}\n    r.appendChild(td(a));return r;\n  });\n  host.appendChild(table(['Player','Where','Joined','Network','Actions'],rows,'Nobody is online right now.'));\n}\nvar AQ='';\nfunction renderAccounts(){\n  var host=$('t-accounts');\n  if(!host.firstChild){var row=el('div','row');var q=el('input');q.placeholder='Search name or email\u2026';q.style.flex='1';q.oninput=function(){AQ=q.value.toLowerCase();fillAccounts()};row.appendChild(q);host.appendChild(row);host.appendChild(el('div'));host.lastChild.id='accbody'}\n  fillAccounts();\n}\nfunction fillAccounts(){\n  var body=$('accbody');if(!body)return;body.textContent='';\n  var list=DATA.accounts.filter(function(a){return !AQ||(a.name+' '+a.email).toLowerCase().indexOf(AQ)>=0});\n  var rows=list.map(function(a){\n    var r=el('tr');var n=el('span');n.appendChild(document.createTextNode((a.name||'(no name yet)')+' '));\n    n.appendChild(tag(a.kind==='google'?'Google':'Guest',a.kind==='google'?'g':''));\n    if(a.pw)n.appendChild(tag('password'));if(a.fr)n.appendChild(tag(a.fr+(a.fr===1?' friend':' friends')));\n    if(a.online)n.appendChild(tag('online','on'));if(a.banned)n.appendChild(tag('banned','b'));if(a.muted)n.appendChild(tag('muted','m'));\n    r.appendChild(td(n));r.appendChild(td(a.email||''));r.appendChild(td(ago(a.created)));r.appendChild(td(ago(a.seen)));\n    var s=el('span');\n    s.appendChild(btn('Rename','',function(){var nn=prompt('New name for '+a.name,a.name);if(nn)act({act:'rename',aid:a.id,name:nn})}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn(a.muted?'Unmute':'Mute','',function(){act({act:a.muted?'unmute':'mute',aid:a.id})}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn(a.banned?'Unban':'Ban',a.banned?'ok':'bad',function(){act({act:a.banned?'unban':'ban',aid:a.id},a.banned?null:'Ban '+a.name+'?')}));s.appendChild(document.createTextNode(' '));\n    if(a.pw){s.appendChild(btn('Clear password','',function(){act({act:'resetpw',aid:a.id},'Clear the password of \"'+a.name+'\"? They can then sign in with their recovery code and set a new one.')}));s.appendChild(document.createTextNode(' '))}\n    s.appendChild(btn('Delete','bad',function(){act({act:'delete',aid:a.id},'Delete the account \"'+a.name+'\" for good? The name becomes free.')}));\n    r.appendChild(td(s));return r;\n  });\n  body.appendChild(table(['Name','Email','Created','Last seen','Actions'],rows,'No accounts yet.'));\n}\nfunction logLine(l){var d=new Date(l.ts),p=function(n){return (n<10?'0':'')+n};return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())+'  '+l.m}\nfunction renderLogs(){\n  var box=$('logs'),f=$('lf').value.toLowerCase(),chat=$('lchat').checked;\n  var stick=box.scrollTop+box.clientHeight>=box.scrollHeight-30;box.textContent='';\n  DATA.logs.forEach(function(l){\n    if(f&&l.m.toLowerCase().indexOf(f)<0)return;\n    var isChat=l.m.indexOf('chat ')===0;if(isChat&&!chat)return;\n    var d=el('div',isChat?'chat':/^admin/.test(l.m)?'admin':/failed|banned|error/.test(l.m)?'warn':'',logLine(l));box.appendChild(d);\n  });\n  if(stick)box.scrollTop=box.scrollHeight;\n}\n$('go').onclick=function(){KEY=$('key').value;api('/admin/api/state').then(function(d){if(d.status===401){showLogin('Wrong key');return}if(d.status===429){showLogin('Too many wrong tries. Wait 10 minutes.');return}try{sessionStorage.setItem('sbk',KEY)}catch(e){}start()})};\n$('key').onkeydown=function(e){if(e.key==='Enter')$('go').onclick()};\n$('out').onclick=function(){try{sessionStorage.removeItem('sbk')}catch(e){}KEY='';showLogin('')};\ndocument.querySelectorAll('nav button').forEach(function(b){b.onclick=function(){\n  TAB=b.dataset.t;document.querySelectorAll('nav button').forEach(function(x){x.classList.toggle('on',x===b)});\n  ['players','accounts','logs','tools'].forEach(function(t){$('t-'+t).hidden=t!==TAB});if(DATA)render();\n}});\n$('lf').oninput=function(){if(DATA)renderLogs()};$('lchat').onchange=function(){if(DATA)renderLogs()};\n$('ldl').onclick=function(){if(!DATA)return;var t=DATA.logs.map(logLine).join('\\n');var a=document.createElement('a');a.href=URL.createObjectURL(new Blob([t],{type:'text/plain'}));a.download='sunset-bay-log.txt';a.click()};\n$('saygo').onclick=function(){var t=$('say').value.trim();if(!t)return;act({act:'say',text:t});$('say').value=''};\nif(KEY)api('/admin/api/state').then(function(d){if(d.status===401||d.status===429)showLogin('');else start()}).catch(function(){showLogin('Cannot reach the server yet. It may be waking up; try again in a minute.')});else showLogin('');\n})();\n</script></body></html>\n";
const failures = new Map();
function adminKeyOk(k) { return crypto.timingSafeEqual(Buffer.from(sha(k)), Buffer.from(sha(ADMIN_KEY))); }
function adminHttp(req, res, url) {
  if (!ADMIN_KEY) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Admin panel is off. Set ADMIN_KEY in the server environment to turn it on.'); }
  if (url === '/admin' || url === '/admin/') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-frame-options': 'DENY' }); return res.end(ADMIN_HTML); }
  const ip = ipOf(req), now = Date.now(), recent = (failures.get(ip) || []).filter(t => now - t < 600000);
  if (recent.length >= 10) return json(res, 429, { ok: false, err: 'Too many wrong keys' });
  if (!adminKeyOk(String(req.headers['x-admin-key'] || ''))) { recent.push(now); failures.set(ip, recent); log('admin sign-in failed', maskIp(ip)); return json(res, 401, { ok: false, err: 'Wrong key' }); }
  if (url === '/admin/api/state') return json(res, 200, adminState());
  if (url === '/admin/api/act' && req.method === 'POST') return readBody(req, res, b => json(res, 200, adminAct(b, ip)));
  json(res, 404, { ok: false });
}
function adminState() {
  const live = new Set([...clients.values()].map(c => c.acc && c.acc.id).filter(Boolean));
  return {
    ok: true, now: Date.now(), tod: +tod.toFixed(2), online: online(), up: process.uptime() | 0, world: counts(), google: !!GOOGLE_ID, requireLogin: REQUIRE_LOGIN,
    players: [...clients.values()].map(c => ({ id: c.id, name: c.name, aid: c.acc ? c.acc.id : '', kind: c.acc ? c.acc.kind : 'none', x: Math.round(c.x), z: Math.round(c.z), area: c.area, since: c.since, ip: c.ipm, muted: !!(c.acc && c.acc.muted) })),
    accountTotal: state.accounts.size,
    accounts: [...state.accounts.values()].sort((a, b) => b.seen - a.seen).slice(0, 500).map(a => ({ id: a.id, name: a.name, kind: a.kind, email: a.email, created: a.created, seen: a.seen, banned: a.banned, muted: a.muted, pw: !!a.pw, fr: a.fr.length, online: live.has(a.id) })),
    logs: LOGS.slice(-300)
  };
}
function adminAct(b, ip) {
  const who = 'admin ' + maskIp(ip) + ':';
  const acc = state.accounts.get(String(b.aid || ''));
  switch (b.act) {
    case 'kick': {
      const c = clients.get(String(b.id || '')); if (!c) return { ok: false, err: 'Player is not online' };
      send(c.ws, { t: 'kicked', why: 'kicked' }); try { c.ws.close(4003, 'kicked'); } catch (e) {} log(who, 'kicked', c.name); return { ok: true };
    }
    case 'ban': if (!acc) return { ok: false, err: 'No such account' }; acc.banned = true; dirtyAcc = true; kickAccount(acc.id, 'banned'); log(who, 'banned', acc.name); return { ok: true };
    case 'unban': if (!acc) return { ok: false, err: 'No such account' }; acc.banned = false; dirtyAcc = true; log(who, 'unbanned', acc.name); return { ok: true };
    case 'mute': if (!acc) return { ok: false, err: 'No such account' }; acc.muted = true; dirtyAcc = true; log(who, 'muted', acc.name); return { ok: true };
    case 'unmute': if (!acc) return { ok: false, err: 'No such account' }; acc.muted = false; dirtyAcc = true; log(who, 'unmuted', acc.name); return { ok: true };
    case 'rename': {
      if (!acc) return { ok: false, err: 'No such account' };
      const r = checkName(b.name, acc.id); if (r.err) return { ok: false, err: r.err };
      const old = acc.name; setName(acc, r.name); log(who, 'renamed', old, '->', r.name);
      for (const c of clients.values()) if (c.acc === acc) { c.name = r.name; send(c.ws, { t: 'name', name: r.name }); broadcast({ t: 'look', id: c.id, look: c.look, name: c.name }, c.ws); }
      return { ok: true };
    }
    case 'resetpw': if (!acc) return { ok: false, err: 'No such account' }; acc.pw = ''; dirtyAcc = true; log(who, 'cleared the password of', acc.name); return { ok: true };
    case 'delete': if (!acc) return { ok: false, err: 'No such account' }; log(who, 'deleted account', acc.name); dropAccount(acc); return { ok: true };
    case 'say': { const t = cleanText(b.text).slice(0, 140); if (!t) return { ok: false, err: 'Empty message' }; log(who, 'announced:', t); pushChat({ sys: true, text: '[Admin] ' + t }, true); return { ok: true }; }
  }
  return { ok: false, err: 'Unknown action' };
}

initStore().then(() => server.listen(PORT, () => console.log('Sunset Bay server listening on', PORT))).catch(e => { console.error('startup failed', e); process.exit(1); });

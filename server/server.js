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
const LIM = { ads: 12, adImg: 230000, pieces: 4000, blocks: 9000, planted: 800, chat: 60, accounts: 5000, friends: 100, requests: 30, dmPairs: 3000, dmKeep: 60 };
const LOGCAP = 400;

/* ---------- world state ---------- */
const state = { pieces: new Map(), blocks: new Map(), planted: new Map(), chat: [], accounts: new Map(), ads: [], saves: new Map(), cfg: { drops: true } };
const dirtySv = new Set(); /* account ids whose cloud save must be written */
const dms = new Map(); /* 'idA|idB' -> [{f: from id, t: time, x: text}] */
const LOGS = [];
const byName = new Map(), byTok = new Map(), bySub = new Map(); /* lookup indexes for accounts */
let dirty = false, dirtyAcc = false, dirtyLog = false, dirtyDm = false, dirtyAd = false, logPend = false;
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
    const d = {}; const sv = []; r.rows.forEach(x => { if (x.key.startsWith('sv:')) sv.push([x.key.slice(3), x.data]); else d[x.key] = x.data; }); d.saves = sv;
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
  (Array.isArray(d.ads) ? d.ads : []).slice(0, LIM.ads).forEach(a => { const c = cleanAd(a, true); if (c) state.ads.push(c); });
  if (d.cfg && typeof d.cfg === 'object') state.cfg.drops = d.cfg.drops !== false;
  (Array.isArray(d.saves) ? d.saves : []).forEach(e => { if (Array.isArray(e) && typeof e[0] === 'string' && e[1] && typeof e[1].data === 'object') state.saves.set(e[0], { cv: e[1].cv | 0, ts: +e[1].ts || 0, data: e[1].data }); });
  state.chat = Array.isArray(d.chat) ? d.chat.slice(-LIM.chat) : [];
  (d.accounts || []).forEach(a => {
    if (!a || typeof a.id !== 'string' || typeof a.name !== 'string') return;
    state.accounts.set(a.id, { id: a.id, name: a.name, kind: a.kind === 'google' ? 'google' : 'guest', sub: String(a.sub || ''), email: String(a.email || ''), th: Array.isArray(a.th) ? a.th.slice(-5) : [], rs: a.rs | 0, adm: !!a.adm, created: +a.created || Date.now(), seen: +a.seen || 0, banned: !!a.banned, muted: !!a.muted, pw: typeof a.pw === 'string' ? a.pw : '', fr: Array.isArray(a.fr) ? a.fr.slice(0, 100) : [], rq: Array.isArray(a.rq) ? a.rq.slice(0, 30) : [], rd: a.rd && typeof a.rd === 'object' ? a.rd : {} });
  });
  (d.dms || []).forEach(e => { if (Array.isArray(e) && typeof e[0] === 'string' && Array.isArray(e[1])) dms.set(e[0], e[1].slice(-LIM.dmKeep)); });
  state.accounts.forEach(a => { if (a.name) byName.set(a.name.toLowerCase(), a.id); if (a.sub) bySub.set(a.sub, a.id); a.th.forEach(h => byTok.set(h, a.id)); });
  if (Array.isArray(d.logs)) d.logs.slice(-LOGCAP).forEach(l => { if (l && typeof l.m === 'string') LOGS.push({ ts: +l.ts || Date.now(), m: l.m.slice(0, 300) }); });
}
const counts = () => `${state.pieces.size} pieces, ${state.blocks.size} blocks, ${state.planted.size} trees`;
const snapshot = () => ({ pieces: [...state.pieces.values()], blocks: [...state.blocks.values()], planted: [...state.planted.values()], chat: state.chat, accounts: [...state.accounts.values()], dms: [...dms.entries()], logs: LOGS, ads: state.ads, cfg: state.cfg, saves: [...state.saves.entries()] });
/* ---------- sponsor billboards (admin only) ---------- */
const IMG_RE = /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+\/=]+$/;
function cleanAd(a, keepStats) {
  if (!a || typeof a !== 'object') return null;
  const id = typeof a.id === 'string' && /^[\w-]{3,24}$/.test(a.id) ? a.id : null; if (!id) return null;
  const link = String(a.link || '').trim().slice(0, 300); if (link && !/^https:\/\/[^\s<>"']+$/i.test(link)) return null;
  const img = String(a.img || ''); if (img && (img.length > LIM.adImg || !IMG_RE.test(img))) return null;
  const hasPos = a.x !== undefined && a.x !== null && a.x !== '' && a.z !== undefined && a.z !== null && a.z !== '';
  if (hasPos && (!num(a.x, -WORLD, WORLD) || !num(a.z, -WORLD, WORLD))) return null;
  if (!hasPos) { a.x = 0; a.z = 0; a.pl = false; }
  return { id, title: cleanText(a.title).slice(0, 40), link, img, x: +(+a.x).toFixed(1), z: +(+a.z).toFixed(1), r: int(a.r, 0, 3) ? a.r : 0, pl: a.pl !== false, on: a.on !== false, views: keepStats ? (+a.views | 0) : 0, clicks: keepStats ? (+a.clicks | 0) : 0, created: +a.created || Date.now() };
}
const publicAds = () => state.ads.filter(a => a.on && a.pl && a.img).map(a => ({ id: a.id, title: a.title, link: a.link, img: a.img, x: a.x, z: a.z, r: a.r }));
const pushAds = () => { broadcast({ t: 'ads', list: publicAds() }); for (const c of clients.values()) if (c.acc && c.acc.adm) send(c.ws, adminAdsMsg()); };
const adminAdsMsg = () => ({ t: 'adlist', list: state.ads.map(a => ({ id: a.id, title: a.title, on: a.on, pl: a.pl, x: a.x, z: a.z, r: a.r, views: a.views, clicks: a.clicks })) });
/* what a joining player may see: the shared world and public chat only (never accounts, tokens, private chats or logs) */
const worldView = () => ({ pieces: [...state.pieces.values()], blocks: [...state.blocks.values()], planted: [...state.planted.values()], chat: state.chat });
let saving = false;
async function save() {
  if (saving || !(dirty || dirtyAcc || dirtyLog || dirtyDm || dirtyAd || dirtySv.size)) return; saving = true;
  const keys = []; if (dirty) keys.push('pieces', 'blocks', 'planted', 'chat'); if (dirtyAcc) keys.push('accounts'); if (dirtyDm) keys.push('dms'); if (dirtyAd) keys.push('ads', 'cfg'); if (dirtyLog) keys.push('logs');
  const svIds = [...dirtySv]; dirtySv.clear();
  const was = { d: dirty, a: dirtyAcc, l: dirtyLog, m: dirtyDm, v: dirtyAd }; dirty = dirtyAcc = dirtyLog = dirtyDm = dirtyAd = false;
  try {
    const sn = snapshot();
    if (pool) {
      for (const id of svIds) {
        const e = state.saves.get(id);
        if (e) await pool.query('INSERT INTO sb_state (key, data, updated) VALUES ($1, $2::jsonb, now()) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated = now()', ['sv:' + id, JSON.stringify(e)]);
        else await pool.query('DELETE FROM sb_state WHERE key = $1', ['sv:' + id]);
      }
      for (const k of keys) await pool.query('INSERT INTO sb_state (key, data, updated) VALUES ($1, $2::jsonb, now()) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated = now()', [k, JSON.stringify(sn[k])]);
    } else fs.writeFileSync(FILE, JSON.stringify(sn));
  } catch (e) { dirty = dirty || was.d; dirtyAcc = dirtyAcc || was.a; dirtyLog = dirtyLog || was.l; dirtyDm = dirtyDm || was.m; dirtyAd = dirtyAd || was.v; svIds.forEach(i => dirtySv.add(i)); console.error('save failed', e.message); }
  saving = false;
}
setInterval(save, 6000);
['SIGTERM', 'SIGINT'].forEach(sig => process.on(sig, async () => { dirty = dirtyAcc = dirtyLog = dirtyDm = dirtyAd = true; await save(); process.exit(0); }));
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
  const a = Object.assign({ id: 'a' + crypto.randomBytes(6).toString('hex'), name: name || '', kind, sub: '', email: '', th: [], created: Date.now(), seen: Date.now(), banned: false, muted: false, pw: '', rs: 0, adm: false, fr: [], rq: [], rd: {} }, extra || {});
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
  if (state.saves.delete(a.id)) dirtySv.add(a.id);
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
function readBody(req, res, cb, max) {
  let n = 0; const parts = []; max = max || 8192;
  req.on('data', ch => { n += ch.length; if (n > max) { req.destroy(); return; } parts.push(ch); });
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
const wss = new WebSocketServer({ server, maxPayload: 128 * 1024 });
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
const pubPlayer = c => ({ id: c.id, name: c.name, look: c.look, x: c.x, y: c.y, z: c.z, h: c.h, sp: c.sp, area: c.area, ps: c.ps | 0 });

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
    send(ws, { t: 'init', id: c.id, name: c.name, acc: c.acc ? 1 : 0, adm: c.acc && c.acc.adm ? 1 : 0, rs: c.acc ? c.acc.rs | 0 : 0, pw: c.acc && c.acc.pw ? 1 : 0, tod, players: [...clients.values(), ...ghosts.values()].filter(o => o !== c).map(pubPlayer), ...worldView(), drops: [...drops.values()], save: c.acc && state.saves.has(c.acc.id) ? { cv: state.saves.get(c.acc.id).cv, data: state.saves.get(c.acc.id).data } : null });
    send(ws, { t: 'ads', list: publicAds() }); if (c.acc && c.acc.adm) send(ws, adminAdsMsg());
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
      c.ps = m.ps === 1 || m.ps === 2 ? m.ps : 0; /* 0 standing, 1 sitting, 2 lying in bed */
      broadcast({ t: 'pos', id: c.id, x: +c.x.toFixed(2), y: +c.y.toFixed(2), z: +c.z.toFixed(2), h: +c.h.toFixed(2), sp: +c.sp.toFixed(1), area: c.area, ps: c.ps }, ws);
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
    case 'dpick': case 'dbad': dropMsg(c, m); break;
    case 'save': saveMsg(c, m); break;
    case 'adreq': if (c.acc && c.acc.adm) send(ws, adminAdsMsg()); break;
    case 'adplace': {
      if (!c.acc || !c.acc.adm) break;
      const a = state.ads.find(x => x.id === String(m.id)); if (!a) break;
      if (m.del) { state.ads.splice(state.ads.indexOf(a), 1); log(c.acc.name, 'deleted billboard', a.title || a.id); }
      else if (m.on !== undefined && m.x === undefined) { a.on = !!m.on; log(c.acc.name, a.on ? 'turned on billboard' : 'turned off billboard', a.title || a.id); }
      else {
        if (!num(m.x, -WORLD, WORLD) || !num(m.z, -WORLD, WORLD) || !int(m.r, 0, 3)) break;
        a.x = +(+m.x).toFixed(1); a.z = +(+m.z).toFixed(1); a.r = m.r; a.pl = true; a.on = true; log(c.acc.name, 'placed billboard', a.title || a.id, 'at', a.x + ',' + a.z);
      }
      dirtyAd = true; pushAds(); break;
    }
    case 'adview': case 'adclick': {
      const a = state.ads.find(x => x.id === String(m.id)); if (!a || !spend(c)) break;
      if (m.t === 'adview') { c.adSeen = c.adSeen || new Set(); if (c.adSeen.has(a.id)) break; c.adSeen.add(a.id); a.views++; } else a.clicks++;
      dirtyAd = true; break;
    }
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
/* ---------- cloud save of player progress (per account; stored as one small row each) ---------- */
const SAVE_MAX = 90000;
const idKey = /^[a-z][a-z0-9_]{1,24}$/;
function cleanSave(d, prev, elapsed) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
  const out = {}, fixed = {};
  const intIn = (v, hi) => { v = +v; return Number.isFinite(v) ? Math.max(0, Math.min(hi, Math.floor(v))) : 0; };
  const el = Math.max(5, Math.min(3600, elapsed || 5));
  /* plausible growth since the last accepted save: generous, but a hand-edited jackpot is cut back */
  const pc = prev ? prev.coins : 0, cmax = prev ? pc + 3000 + 40 * el : 20000;
  out.coins = intIn(d.coins, 2e6); if (out.coins > cmax) { out.coins = Math.min(out.coins, Math.floor(cmax)); fixed.coins = out.coins; }
  const px = prev ? prev.xp : 0, xmax = prev ? px + 2000 + 60 * el : 50000;
  out.xp = intIn(d.xp, 1e7); if (out.xp > xmax) { out.xp = Math.floor(xmax); fixed.xp = out.xp; }
  out.inv = {}; let n = 0;
  if (d.inv && typeof d.inv === 'object') for (const k of Object.keys(d.inv)) {
    if (!idKey.test(k) || ++n > 150) continue;
    let v = intIn(d.inv[k], 99999); const pv = prev && prev.inv ? (prev.inv[k] | 0) : 0, imax = prev ? pv + 1500 + 15 * el : 1000;
    if (v > imax) { v = Math.floor(imax); fixed.inv = fixed.inv || {}; fixed.inv[k] = v; }
    if (v > 0) out.inv[k] = v;
  }
  out.hotbar = (Array.isArray(d.hotbar) ? d.hotbar : []).slice(0, 9).map(h => (typeof h === 'string' && idKey.test(h)) ? h : null);
  out.qi = intIn(d.qi, 500);
  for (const k of ['qprog', 'farm', 'look', 'grids']) if (d[k] !== undefined && d[k] !== null) { try { const j = JSON.stringify(d[k]); if (j.length <= (k === 'grids' ? 50000 : k === 'farm' ? 30000 : 6000)) out[k] = JSON.parse(j); } catch (e) {} }
  if (out.grids && typeof out.grids === 'object') delete out.grids.world; /* the shared world belongs to the server */
  return { out, fixed: Object.keys(fixed).length ? fixed : null };
}
function saveMsg(c, m) {
  if (!c.acc || !m || typeof m.data !== 'object') return;
  if (JSON.stringify(m.data).length > SAVE_MAX) { send(c.ws, { t: 'err', msg: 'Save too large' }); return; }
  const prev = state.saves.get(c.acc.id), now = Date.now();
  if (prev && now - prev.ts < 8000) { send(c.ws, { t: 'saved', cv: prev.cv, busy: true }); return; }
  const r = cleanSave(m.data, prev && prev.data, prev ? (now - prev.ts) / 1000 : 0); if (!r) return;
  const cv = (prev ? prev.cv : 0) + 1;
  state.saves.set(c.acc.id, { cv, ts: now, data: r.out }); dirtySv.add(c.acc.id);
  if (r.fixed) log(c.acc.name, 'progress save cut back (unusual jump):', JSON.stringify(r.fixed).slice(0, 120));
  send(c.ws, { t: 'saved', cv, fix: r.fixed ? Object.assign({ coins: r.out.coins, xp: r.out.xp }, r.fixed.inv ? { inv: r.out.inv } : {}) : undefined });
}
/* ---------- world drops: fruits and mystery boxes that appear at random places (not saved; they are just for fun) ---------- */
const DROP_MAX = +process.env.DROP_MAX || 10, DROP_EVERY = +process.env.DROP_EVERY || 20000, DROP_LIFE = 8 * 60000;
const drops = new Map(); let nextDrop = 1;
/* seasons follow the real calendar: summer Mar-Jun, monsoon Jul-Oct, winter Nov-Feb */
const SEASONS = { summer: ['mango', 'jackfruit', 'banana', 'carrot'], monsoon: ['banana', 'jackfruit', 'apple', 'carrot'], winter: ['apple', 'carrot', 'pumpkin', 'banana'] };
const seasonNow = () => { const m = new Date().getUTCMonth() + 1; return m >= 3 && m <= 6 ? 'summer' : m >= 7 && m <= 10 ? 'monsoon' : 'winter'; };
function pickKind() {
  if (Math.random() < .04) return Math.random() < .5 ? 'gfruit' : 'gbox'; /* rare golden drops */
  if (Math.random() < .2) return 'box';
  const k = SEASONS[seasonNow()]; return k[Math.floor(Math.random() * k.length)];
}
const compass = (x, z) => { const d = Math.hypot(x, z); if (d < 45) return 'near the town'; const a = Math.atan2(-z, x) * 180 / Math.PI; const names = ['east', 'north-east', 'north', 'north-west', 'west', 'south-west', 'south', 'south-east']; return (d > 130 ? 'far ' : '') + names[Math.round(((a + 360) % 360) / 45) % 8]; };
function spawnDrop() {
  const live = [...clients.values()]; if (!state.cfg.drops || !live.length || drops.size >= DROP_MAX) return;
  const c = live[Math.floor(Math.random() * live.length)], a = Math.random() * Math.PI * 2, r = 14 + Math.random() * 60;
  const x = Math.max(-WORLD + 5, Math.min(WORLD - 5, c.x + Math.cos(a) * r)), z = Math.max(-WORLD + 5, Math.min(WORLD - 5, c.z + Math.sin(a) * r));
  addDrop(x, z, pickKind());
}
function addDrop(x, z, kind) {
  const d = { id: 'd' + (nextDrop++), kind, x: +x.toFixed(1), z: +z.toFixed(1), ts: Date.now() };
  drops.set(d.id, d); broadcast({ t: 'dspawn', d });
  if (kind === 'gbox' || kind === 'gfruit') pushChat({ sys: true, text: '\u2728 A golden ' + (kind === 'gbox' ? 'mystery box' : 'fruit') + ' appeared ' + compass(d.x, d.z) + ' of the bay!' }, true);
}
setInterval(() => {
  const now = Date.now();
  for (const d of drops.values()) if (now - d.ts > DROP_LIFE) { drops.delete(d.id); broadcast({ t: 'drem', id: d.id }); }
  spawnDrop();
}, DROP_EVERY);
const rnd = (a, b) => a + Math.floor(Math.random() * (b - a + 1));
const GIFTS = ['bread', 'fruit_salad', 'pumpkin_pie', 'wheat_seed', 'carrot_seed', 'tomato_seed', 'strawberry_seed', 'pumpkin_seed', 'sapling'];
function dropReward(kind) {
  if (kind === 'gbox') return { coins: rnd(120, 300), item: GIFTS[rnd(0, GIFTS.length - 1)], n: rnd(2, 4) };
  if (kind === 'gfruit') return { coins: rnd(40, 100), item: 'fruit_salad', n: 1 };
  if (kind === 'box') return Math.random() < .6 ? { coins: rnd(20, 80) } : { item: GIFTS[rnd(0, GIFTS.length - 1)], n: rnd(1, 3), coins: rnd(0, 15) };
  return { item: kind, n: kind === 'pumpkin' ? 1 : rnd(1, 3), coins: Math.random() < .25 ? rnd(2, 8) : 0 };
}
function dropMsg(c, m) {
  if (m.t === 'dbad') { const d = drops.get(String(m.id)); if (d && Date.now() - d.ts < 15000) { drops.delete(d.id); broadcast({ t: 'drem', id: d.id }); spawnDrop(); } return; }
  const d = drops.get(String(m.id)); if (!d) return;
  if (Math.hypot(c.x - d.x, c.z - d.z) > 9) return; /* must really be standing close (a little slack for lag) */
  drops.delete(d.id); broadcast({ t: 'drem', id: d.id, by: c.id });
  send(c.ws, Object.assign({ t: 'dgot', kind: d.kind }, dropReward(d.kind)));
}

/* ---------- admin: reset the world ---------- */
async function backupWorld() {
  const w = { pieces: [...state.pieces.values()], blocks: [...state.blocks.values()], planted: [...state.planted.values()], at: Date.now() };
  if (pool) await pool.query("INSERT INTO sb_state (key, data, updated) VALUES ('world_backup', $1::jsonb, now()) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated = now()", [JSON.stringify(w)]);
  else fs.writeFileSync(FILE + '.world-backup.json', JSON.stringify(w));
}
async function resetWorld(who, clearChat) {
  await backupWorld();
  const was = counts();
  state.pieces.clear(); state.blocks.clear(); state.planted.clear(); drops.clear();
  if (clearChat) state.chat = [];
  dirty = true; log(who, 'RESET THE WORLD (was ' + was + (clearChat ? ', chat cleared' : '') + '; backup saved)');
  broadcast({ t: 'wreset', chat: !!clearChat });
  await save();
}
const ADMIN_HTML = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Sunset Bay Admin</title>\n<style>\n:root{--bg:#1d1429;--panel:#2a1d3b;--line:#46345e;--ink:#fff1e3;--dim:#b9a6cf;--peach:#ffb48a;--ok:#9be3c1;--bad:#ff7a8a;--warn:#ffd27a}\n*{box-sizing:border-box}\n[hidden]{display:none!important}\nbody{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,\"Segoe UI\",sans-serif}\nheader{display:flex;flex-wrap:wrap;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--panel)}\nh1{font-size:17px;margin:0;margin-right:auto}h1 span{color:var(--peach)}\n.pill{padding:3px 10px;border-radius:99px;background:rgba(255,255,255,.08);color:var(--dim);font-size:12px;font-weight:700}\n.pill b{color:var(--ink)}\nmain{padding:14px 16px;max-width:1100px;margin:0 auto}\nnav{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}\nbutton,input,select{font:inherit;color:var(--ink);background:rgba(255,255,255,.08);border:1px solid var(--line);border-radius:8px;padding:6px 12px}\nbutton{cursor:pointer;font-weight:700}button:hover{background:rgba(255,255,255,.16)}\nbutton.on{background:var(--peach);color:#2a1b3d;border-color:var(--peach)}\nbutton.bad{border-color:var(--bad);color:var(--bad)}button.ok{border-color:var(--ok);color:var(--ok)}\nbutton.sm{padding:2px 9px;font-size:12px}\ntable{width:100%;border-collapse:collapse}th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:middle}\nth{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.08em}\n.tag{display:inline-block;padding:1px 8px;border-radius:99px;font-size:11px;font-weight:800;background:rgba(255,255,255,.1);color:var(--dim)}\n.tag.g{background:#3a5a8a;color:#dbe8ff}.tag.b{background:#5a2a3a;color:var(--bad)}.tag.m{background:#5a4a2a;color:var(--warn)}.tag.on{background:#25503f;color:var(--ok)}\n.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px}\n#logs{font:12px/1.5 ui-monospace,Consolas,monospace;background:rgba(0,0,0,.3);border:1px solid var(--line);border-radius:10px;padding:10px;max-height:62vh;overflow:auto;white-space:pre-wrap;word-break:break-word}\n#logs div.chat{color:#ffd9bf}#logs div.admin{color:var(--warn)}#logs div.warn{color:var(--bad)}\n#login{max-width:340px;margin:12vh auto;background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:20px;display:flex;flex-direction:column;gap:10px}\n#login p{margin:0;color:var(--dim)}\n.err{color:var(--bad);min-height:1.3em}\n.empty{color:var(--dim);padding:14px 0}\n</style></head><body>\n<div id=\"login\" hidden>\n  <h1>Sunset Bay <span>Admin</span></h1>\n  <p>Enter the admin key you set on the server.</p>\n  <input id=\"key\" type=\"password\" placeholder=\"Admin key\" autocomplete=\"current-password\">\n  <button id=\"go\" class=\"on\">Open</button>\n  <div class=\"err\" id=\"lerr\"></div>\n</div>\n<div id=\"app\" hidden>\n<header>\n  <h1>Sunset Bay <span>Admin</span></h1>\n  <span class=\"pill\">Online <b id=\"sOnline\">0</b></span>\n  <span class=\"pill\">Accounts <b id=\"sAcc\">0</b></span>\n  <span class=\"pill\">Game time <b id=\"sTod\">--</b></span>\n  <span class=\"pill\">Up <b id=\"sUp\">--</b></span>\n  <span class=\"pill\" id=\"sWorld\"></span>\n  <button class=\"sm\" id=\"out\">Sign out</button>\n</header>\n<main>\n  <nav><button data-t=\"players\" class=\"on\">Players</button><button data-t=\"accounts\">Accounts</button><button data-t=\"logs\">Logs</button><button data-t=\"ads\">Ads</button><button data-t=\"tools\">Tools</button></nav>\n  <section id=\"t-players\"></section>\n  <section id=\"t-accounts\" hidden></section>\n  <section id=\"t-logs\" hidden>\n    <div class=\"row\"><input id=\"lf\" placeholder=\"Filter logs (name, word)\u2026\" style=\"flex:1;min-width:160px\"><label><input type=\"checkbox\" id=\"lchat\" checked> chat</label><button class=\"sm\" id=\"ldl\">Download</button></div>\n    <div id=\"logs\"></div>\n  </section>\n  <section id=\"t-ads\" hidden>\n    <p class=\"empty\" style=\"padding:0 0 10px\">Sponsor billboards stand in the world with a small \"Sponsored\" label. Players near one see the title and can tap to open the link (they are asked first). Only you can create them.</p>\n    <div class=\"row\"><input id=\"adTitle\" placeholder=\"Title (shown to players)\" maxlength=\"40\" style=\"flex:1;min-width:180px\"><input id=\"adLink\" placeholder=\"Link (https://...) optional\" style=\"flex:2;min-width:220px\"></div>\n    <div class=\"row\"><input id=\"adFile\" type=\"file\" accept=\"image/*\"><span id=\"adImgInfo\" class=\"empty\" style=\"padding:0\">no image chosen</span></div>\n    <div class=\"row\"><label>Place at player <select id=\"adWho\"><option value=\"\">(leave empty - place it in the game)</option></select></label><label>x <input id=\"adX\" type=\"number\" style=\"width:80px\"></label><label>z <input id=\"adZ\" type=\"number\" style=\"width:80px\"></label>\n      <label>Faces <select id=\"adR\"><option value=\"0\">south</option><option value=\"2\">north</option><option value=\"1\">east</option><option value=\"3\">west</option></select></label></div>\n    <div class=\"row\"><button id=\"adSave\" class=\"on\">Save billboard</button><button id=\"adClear\">New</button><span class=\"err\" id=\"adErr\"></span></div>\n    <div id=\"adList\"></div>\n  </section>\n  <section id=\"t-tools\" hidden>\n    <div class=\"row\"><input id=\"say\" placeholder=\"Announcement to all players\" maxlength=\"140\" style=\"flex:1;min-width:200px\"><button id=\"saygo\" class=\"on\">Send</button></div>\n    <p class=\"empty\">Announcements appear in everyone's chat as [Admin]. Kick, ban and mute are on the Players and Accounts tabs.</p>\n    <h2 style=\"font-size:15px;margin:22px 0 6px\">Game admin account</h2>\n    <p class=\"empty\">A game admin sees a Billboards button in the game and can place, move, turn off or delete billboards where they stand. Sign in to the game with that account. Current: <b id=\"gaList\">none</b></p>\n    <div class=\"row\"><input id=\"gaName\" placeholder=\"Account name\" style=\"min-width:180px\"><button id=\"gaOn\" class=\"on\">Make game admin</button><button id=\"gaOff\">Remove</button><span id=\"gaMsg\" class=\"empty\"></span></div>\n    <h2 style=\"font-size:15px;margin:22px 0 6px\">Fruit rain</h2>\n    <p class=\"empty\" style=\"padding:0 0 10px\">Drops 12 fruits and boxes around the online players and tells everyone in chat. Season now: <b id=\"sSeason\">-</b>.</p>\n    <div class=\"row\"><button id=\"rainGo\" class=\"ok\">Start fruit rain</button><button id=\"dropsSw\"></button><span id=\"dropsSt\" class=\"empty\" style=\"padding:0\"></span></div>\n    <h2 style=\"font-size:15px;margin:22px 0 6px\">Player progress (cloud save)</h2>\n    <p class=\"empty\" style=\"padding:0 0 10px\">Saved progress: <b id=\"sSaves\">0</b> players. Resetting clears a player's coins, items, XP and quests on the server and on their device; they are signed out and start fresh.</p>\n    <div class=\"row\"><input id=\"rsName\" placeholder=\"Player name\" style=\"min-width:180px\"><button id=\"rsGo\" class=\"bad\">Reset this player's progress</button></div>\n    <div class=\"row\"><input id=\"rsAllType\" placeholder=\"Type RESET for everyone\" style=\"min-width:200px\"><button id=\"rsAll\" class=\"bad\">Reset everyone's progress</button></div>\n    <div class=\"err\" id=\"rsErr\"></div>\n    <h2 style=\"font-size:15px;margin:22px 0 6px;color:var(--bad)\">Reset world</h2>\n    <p class=\"empty\" style=\"padding:0 0 10px\">Removes every built piece, block and planted tree for everyone. Accounts, passwords and friends stay. A backup of the old world is saved on the server first. Players online are refreshed to the empty world at once.</p>\n    <div class=\"row\"><label><input type=\"checkbox\" id=\"rchat\"> also clear the public chat history</label></div>\n    <div class=\"row\"><input id=\"rtype\" placeholder=\"Type RESET to confirm\" style=\"min-width:200px\"><button id=\"rgo\" class=\"bad\">Reset world</button></div>\n    <div class=\"err\" id=\"rerr\"></div>\n  </section>\n</main></div>\n<script>\n(function(){\nvar KEY='';try{KEY=sessionStorage.getItem('sbk')||''}catch(e){}\nvar DATA=null,TAB='players',timer=null;\nfunction $(i){return document.getElementById(i)}\nfunction el(tag,cls,txt){var e=document.createElement(tag);if(cls)e.className=cls;if(txt!=null)e.textContent=txt;return e}\nfunction ago(ts){if(!ts)return 'never';var s=Math.max(0,(Date.now()-ts)/1000|0);if(s<60)return s+'s ago';if(s<3600)return (s/60|0)+'m ago';if(s<86400)return (s/3600|0)+'h ago';return (s/86400|0)+'d ago'}\nfunction api(path,body){\n  var o={headers:{'x-admin-key':KEY,'content-type':'application/json'}};\n  if(body){o.method='POST';o.body=JSON.stringify(body)}\n  return fetch(path,o).then(function(r){return r.json().then(function(j){j.status=r.status;return j})});\n}\nfunction showLogin(msg){$('app').hidden=true;$('login').hidden=false;$('lerr').textContent=msg||'';clearInterval(timer)}\nfunction start(){$('login').hidden=true;$('app').hidden=false;poll();clearInterval(timer);timer=setInterval(poll,3000)}\nfunction poll(){\n  if(document.hidden)return;\n  api('/admin/api/state').then(function(d){\n    if(d.status===401){showLogin('Wrong key');return}\n    if(d.status===429){showLogin('Too many wrong tries. Wait 10 minutes.');return}\n    if(!d.ok)return;DATA=d;render();\n  }).catch(function(){});\n}\nfunction act(o,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;api('/admin/api/act',o).then(function(r){if(!r.ok)alert(r.err||'Failed');poll()})}\nfunction render(){\n  var d=DATA;$('sOnline').textContent=d.online;$('sAcc').textContent=d.accountTotal;\n  var h=Math.floor(d.tod),m=Math.floor((d.tod-h)*60);$('sTod').textContent=(h<10?'0':'')+h+':'+(m<10?'0':'')+m;\n  $('sUp').textContent=d.up>3600?(d.up/3600).toFixed(1)+'h':Math.floor(d.up/60)+'m';\n  $('sWorld').textContent=d.world+(d.google?' \u00b7 Google on':' \u00b7 Google off')+(d.requireLogin?'':' \u00b7 login optional');\n  if(TAB==='players')renderPlayers();if(TAB==='accounts')renderAccounts();if(TAB==='logs')renderLogs();if(TAB==='ads')renderAds();$('sSeason').textContent=d.season+' ('+d.drops+' drops out now)';$('dropsSt').textContent=d.dropsOn?'Drops are ON':'Drops are OFF';$('dropsSw').textContent=d.dropsOn?'Switch drops off':'Switch drops on';$('dropsSw').className=d.dropsOn?'bad':'ok';window.DROPS_ON=d.dropsOn;$('gaList').textContent=(d.gameAdmins&&d.gameAdmins.length)?d.gameAdmins.join(', '):'none';$('sSaves').textContent=d.saves;\n}\nfunction table(heads,rows,emptyTxt){\n  if(!rows.length)return el('div','empty',emptyTxt);\n  var t=el('table'),hr=el('tr');heads.forEach(function(x){hr.appendChild(el('th',null,x))});t.appendChild(hr);\n  rows.forEach(function(r){t.appendChild(r)});return t;\n}\nfunction td(x){var c=el('td');if(x&&x.nodeType)c.appendChild(x);else c.textContent=x==null?'':x;return c}\nfunction tag(txt,cls){return el('span','tag '+(cls||''),txt)}\nfunction btn(txt,cls,fn){var b=el('button','sm '+(cls||''),txt);b.onclick=fn;return b}\nfunction renderPlayers(){\n  var host=$('t-players');host.textContent='';\n  var rows=DATA.players.map(function(p){\n    var r=el('tr');\n    var name=el('span');name.appendChild(document.createTextNode(p.name+' '));if(p.kind==='google')name.appendChild(tag('Google','g'));else if(p.kind==='guest')name.appendChild(tag('Guest'));else name.appendChild(tag('No account'));if(p.muted)name.appendChild(tag('muted','m'));\n    r.appendChild(td(name));r.appendChild(td(p.area+' ('+p.x+', '+p.z+')'));r.appendChild(td(ago(p.since)));r.appendChild(td(p.ip));\n    var a=el('span');\n    a.appendChild(btn('Kick','',function(){act({act:'kick',id:p.id},'Kick '+p.name+'?')}));a.appendChild(document.createTextNode(' '));\n    if(p.aid){a.appendChild(btn(p.muted?'Unmute':'Mute','',function(){act({act:p.muted?'unmute':'mute',aid:p.aid})}));a.appendChild(document.createTextNode(' '));a.appendChild(btn('Ban','bad',function(){act({act:'ban',aid:p.aid},'Ban '+p.name+'? They cannot sign in again until unbanned.')}))}\n    r.appendChild(td(a));return r;\n  });\n  host.appendChild(table(['Player','Where','Joined','Network','Actions'],rows,'Nobody is online right now.'));\n}\nvar AQ='';\nfunction renderAccounts(){\n  var host=$('t-accounts');\n  if(!host.firstChild){var row=el('div','row');var q=el('input');q.placeholder='Search name or email\u2026';q.style.flex='1';q.oninput=function(){AQ=q.value.toLowerCase();fillAccounts()};row.appendChild(q);host.appendChild(row);host.appendChild(el('div'));host.lastChild.id='accbody'}\n  fillAccounts();\n}\nfunction fillAccounts(){\n  var body=$('accbody');if(!body)return;body.textContent='';\n  var list=DATA.accounts.filter(function(a){return !AQ||(a.name+' '+a.email).toLowerCase().indexOf(AQ)>=0});\n  var rows=list.map(function(a){\n    var r=el('tr');var n=el('span');n.appendChild(document.createTextNode((a.name||'(no name yet)')+' '));\n    n.appendChild(tag(a.kind==='google'?'Google':'Guest',a.kind==='google'?'g':''));\n    if(a.pw)n.appendChild(tag('password'));if(a.fr)n.appendChild(tag(a.fr+(a.fr===1?' friend':' friends')));\n    if(a.online)n.appendChild(tag('online','on'));if(a.banned)n.appendChild(tag('banned','b'));if(a.muted)n.appendChild(tag('muted','m'));\n    r.appendChild(td(n));r.appendChild(td(a.email||''));r.appendChild(td(ago(a.created)));r.appendChild(td(ago(a.seen)));\n    var s=el('span');\n    s.appendChild(btn('Rename','',function(){var nn=prompt('New name for '+a.name,a.name);if(nn)act({act:'rename',aid:a.id,name:nn})}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn(a.muted?'Unmute':'Mute','',function(){act({act:a.muted?'unmute':'mute',aid:a.id})}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn(a.banned?'Unban':'Ban',a.banned?'ok':'bad',function(){act({act:a.banned?'unban':'ban',aid:a.id},a.banned?null:'Ban '+a.name+'?')}));s.appendChild(document.createTextNode(' '));\n    if(a.pw){s.appendChild(btn('Clear password','',function(){act({act:'resetpw',aid:a.id},'Clear the password of \"'+a.name+'\"? They can then sign in with their recovery code and set a new one.')}));s.appendChild(document.createTextNode(' '))}\n    s.appendChild(btn('Delete','bad',function(){act({act:'delete',aid:a.id},'Delete the account \"'+a.name+'\" for good? The name becomes free.')}));\n    r.appendChild(td(s));return r;\n  });\n  body.appendChild(table(['Name','Email','Created','Last seen','Actions'],rows,'No accounts yet.'));\n}\nfunction logLine(l){var d=new Date(l.ts),p=function(n){return (n<10?'0':'')+n};return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())+'  '+l.m}\nfunction renderLogs(){\n  var box=$('logs'),f=$('lf').value.toLowerCase(),chat=$('lchat').checked;\n  var stick=box.scrollTop+box.clientHeight>=box.scrollHeight-30;box.textContent='';\n  DATA.logs.forEach(function(l){\n    if(f&&l.m.toLowerCase().indexOf(f)<0)return;\n    var isChat=l.m.indexOf('chat ')===0;if(isChat&&!chat)return;\n    var d=el('div',isChat?'chat':/^admin/.test(l.m)?'admin':/failed|banned|error/.test(l.m)?'warn':'',logLine(l));box.appendChild(d);\n  });\n  if(stick)box.scrollTop=box.scrollHeight;\n}\n$('go').onclick=function(){KEY=$('key').value;api('/admin/api/state').then(function(d){if(d.status===401){showLogin('Wrong key');return}if(d.status===429){showLogin('Too many wrong tries. Wait 10 minutes.');return}try{sessionStorage.setItem('sbk',KEY)}catch(e){}start()})};\n$('key').onkeydown=function(e){if(e.key==='Enter')$('go').onclick()};\n$('out').onclick=function(){try{sessionStorage.removeItem('sbk')}catch(e){}KEY='';showLogin('')};\ndocument.querySelectorAll('nav button').forEach(function(b){b.onclick=function(){\n  TAB=b.dataset.t;document.querySelectorAll('nav button').forEach(function(x){x.classList.toggle('on',x===b)});\n  ['players','accounts','logs','ads','tools'].forEach(function(t){$('t-'+t).hidden=t!==TAB});if(DATA)render();\n}});\n$('lf').oninput=function(){if(DATA)renderLogs()};$('lchat').onchange=function(){if(DATA)renderLogs()};\n$('ldl').onclick=function(){if(!DATA)return;var t=DATA.logs.map(logLine).join('\\n');var a=document.createElement('a');a.href=URL.createObjectURL(new Blob([t],{type:'text/plain'}));a.download='sunset-bay-log.txt';a.click()};\n$('rgo').onclick=function(){var t=$('rtype').value.trim();if(t!=='RESET'){$('rerr').textContent='Type RESET (capital letters) first.';return}if(!confirm('Really erase the whole world? This cannot be undone from the game.'))return;$('rerr').textContent='';api('/admin/api/act',{act:'resetworld',confirm:'RESET',chat:$('rchat').checked}).then(function(r){$('rerr').style.color=r.ok?'var(--ok)':'var(--bad)';$('rerr').textContent=r.ok?'World reset. A backup was saved.':(r.err||'Failed');$('rtype').value='';poll()})};\nvar AD={id:'',img:'',r:0};\nfunction adFill(a){AD={id:a?a.id:'',img:undefined,r:a?a.r:0};$('adTitle').value=a?a.title:'';$('adLink').value=a?a.link:'';$('adX').value=a&&a.pl?a.x:'';$('adZ').value=a&&a.pl?a.z:'';$('adR').value=String(a?a.r:0);$('adFile').value='';$('adImgInfo').textContent=a?'keeping the current image ('+a.kb+' KB) unless you choose a new one':'no image chosen';$('adErr').textContent=''}\n$('adClear').onclick=function(){adFill(null)};\n$('adWho').onchange=function(){var v=this.value;if(v){var p=v.split(',');$('adX').value=p[0];$('adZ').value=p[1]}};\n$('adFile').onchange=function(){\n  var f=this.files[0];if(!f)return;var rd=new FileReader();\n  rd.onload=function(){var im=new Image();im.onload=function(){\n    var w=Math.min(640,im.width),hh=Math.round(im.height*w/im.width),cv=document.createElement('canvas');cv.width=w;cv.height=hh;\n    var cx=cv.getContext('2d');cx.fillStyle='#fff';cx.fillRect(0,0,w,hh);cx.drawImage(im,0,0,w,hh);\n    var q=.85,u=cv.toDataURL('image/jpeg',q);while(u.length>200000&&q>.35){q-=.1;u=cv.toDataURL('image/jpeg',q)}\n    if(u.length>220000){$('adImgInfo').textContent='Image is too detailed; use a simpler or smaller picture';return}\n    AD.img=u;$('adImgInfo').textContent='ready: '+w+'x'+hh+', '+Math.round(u.length/1024)+' KB'};im.src=rd.result};rd.readAsDataURL(f)};\n$('adSave').onclick=function(){\n  var ad={id:AD.id||undefined,title:$('adTitle').value,link:$('adLink').value.trim(),x:isNaN(parseFloat($('adX').value))?null:parseFloat($('adX').value),z:isNaN(parseFloat($('adZ').value))?null:parseFloat($('adZ').value),r:parseInt($('adR').value,10)};\n  if(AD.img!==undefined)ad.img=AD.img;\n  if(!AD.id&&!ad.img){$('adErr').textContent='Choose an image first';return}\n  if(!isFinite(ad.x)||!isFinite(ad.z)){$('adErr').textContent='Pick a player or type x and z';return}\n  api('/admin/api/act',{act:'adsave',ad:ad}).then(function(r){$('adErr').style.color=r.ok?'var(--ok)':'var(--bad)';$('adErr').textContent=r.ok?'Saved':(r.err||'Failed');if(r.ok)adFill(null);poll()})};\nfunction renderAds(){\n  var sel=$('adWho'),cur=sel.value;sel.textContent='';var o=el('option',null,'(type x z)');o.value='';sel.appendChild(o);\n  DATA.players.forEach(function(p){var o2=el('option',null,p.name+' ('+p.x+', '+p.z+')');o2.value=p.x+','+p.z;sel.appendChild(o2)});sel.value=cur;\n  var host=$('adList');host.textContent='';\n  var rows=DATA.ads.map(function(a){var r=el('tr');\n    var n=el('span');n.appendChild(document.createTextNode((a.title||'(no title)')+' '));n.appendChild(tag(a.on?'on':'off',a.on?'on':''));\n    r.appendChild(td(n));r.appendChild(td(a.x+', '+a.z));r.appendChild(td(a.views+' seen / '+a.clicks+' taps'));r.appendChild(td(a.link?a.link.slice(0,40):'(no link)'));\n    var s=el('span');\n    s.appendChild(btn(a.on?'Turn off':'Turn on','',function(){act({act:'adsave',ad:{id:a.id,on:!a.on}})}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn('Edit','',function(){adFill(a);window.scrollTo(0,0)}));s.appendChild(document.createTextNode(' '));\n    s.appendChild(btn('Delete','bad',function(){act({act:'addel',id:a.id},'Delete the billboard \\\"'+a.title+'\\\"?')}));\n    r.appendChild(td(s));return r});\n  host.appendChild(table(['Billboard','Position','Views','Link','Actions'],rows,'No billboards yet.'));\n}\nfunction gaSet(on){var n=$('gaName').value.trim();if(!n){$('gaMsg').textContent='Type the account name';return}api('/admin/api/act',{act:'setadm',name:n,on:on}).then(function(r){$('gaMsg').style.color=r.ok?'var(--ok)':'var(--bad)';$('gaMsg').textContent=r.ok?'Done':(r.err||'Failed');poll()})}\n$('gaOn').onclick=function(){gaSet(true)};$('gaOff').onclick=function(){gaSet(false)};\n$('dropsSw').onclick=function(){act({act:'dropsswitch',on:!window.DROPS_ON})};\n$('rsGo').onclick=function(){var n=$('rsName').value.trim();if(!n){$('rsErr').textContent='Type the player name';return}$('rsErr').textContent='';api('/admin/api/act',{act:'resetsave',name:n}).then(function(r){$('rsErr').style.color=r.ok?'var(--ok)':'var(--bad)';$('rsErr').textContent=r.ok?'Progress reset':(r.err||'Failed');poll()})};\n$('rsAll').onclick=function(){var t=$('rsAllType').value.trim();if(t!=='RESET'){$('rsErr').style.color='var(--bad)';$('rsErr').textContent='Type RESET (capital letters) first.';return}if(!confirm('Erase the saved progress of EVERY player? Coins, items, XP and quests are lost.'))return;api('/admin/api/act',{act:'resetallsaves',confirm:'RESET'}).then(function(r){$('rsErr').style.color=r.ok?'var(--ok)':'var(--bad)';$('rsErr').textContent=r.ok?('Reset '+r.n+' saves'):(r.err||'Failed');$('rsAllType').value='';poll()})};\n$('rainGo').onclick=function(){act({act:'fruitrain'},'Start a fruit rain for everyone online?')};\n$('saygo').onclick=function(){var t=$('say').value.trim();if(!t)return;act({act:'say',text:t});$('say').value=''};\nif(KEY)api('/admin/api/state').then(function(d){if(d.status===401||d.status===429)showLogin('');else start()}).catch(function(){showLogin('Cannot reach the server yet. It may be waking up; try again in a minute.')});else showLogin('');\n})();\n</script></body></html>\n";
const failures = new Map();
function adminKeyOk(k) { return crypto.timingSafeEqual(Buffer.from(sha(k)), Buffer.from(sha(ADMIN_KEY))); }
function adminHttp(req, res, url) {
  if (!ADMIN_KEY) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Admin panel is off. Set ADMIN_KEY in the server environment to turn it on.'); }
  if (url === '/admin' || url === '/admin/') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-frame-options': 'DENY' }); return res.end(ADMIN_HTML); }
  const ip = ipOf(req), now = Date.now(), recent = (failures.get(ip) || []).filter(t => now - t < 600000);
  if (recent.length >= 10) return json(res, 429, { ok: false, err: 'Too many wrong keys' });
  if (!adminKeyOk(String(req.headers['x-admin-key'] || ''))) { recent.push(now); failures.set(ip, recent); log('admin sign-in failed', maskIp(ip)); return json(res, 401, { ok: false, err: 'Wrong key' }); }
  if (url === '/admin/api/state') return json(res, 200, adminState());
  if (url === '/admin/api/act' && req.method === 'POST') return readBody(req, res, b => json(res, 200, adminAct(b, ip)), 400000);
  json(res, 404, { ok: false });
}
function adminState() {
  const live = new Set([...clients.values()].map(c => c.acc && c.acc.id).filter(Boolean));
  return {
    ok: true, now: Date.now(), tod: +tod.toFixed(2), online: online(), up: process.uptime() | 0, world: counts(), google: !!GOOGLE_ID, requireLogin: REQUIRE_LOGIN,
    players: [...clients.values()].map(c => ({ id: c.id, name: c.name, aid: c.acc ? c.acc.id : '', kind: c.acc ? c.acc.kind : 'none', x: Math.round(c.x), z: Math.round(c.z), area: c.area, since: c.since, ip: c.ipm, muted: !!(c.acc && c.acc.muted) })),
    accountTotal: state.accounts.size, season: seasonNow(), drops: drops.size, dropsOn: state.cfg.drops, saves: state.saves.size, gameAdmins: [...state.accounts.values()].filter(a => a.adm).map(a => a.name),
    ads: state.ads.map(a => ({ id: a.id, title: a.title, link: a.link, x: a.x, z: a.z, r: a.r, pl: a.pl, on: a.on, views: a.views, clicks: a.clicks, kb: Math.round(a.img.length / 1024) })),
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
    case 'resetworld': {
      if (String(b.confirm || '') !== 'RESET') return { ok: false, err: 'Type RESET to confirm' };
      resetWorld(who, !!b.chat).catch(e => console.error('reset failed', e.message));
      return { ok: true };
    }
    case 'dropsswitch': { state.cfg.drops = !!b.on; dirtyAd = true; if (!state.cfg.drops) { for (const d of drops.values()) broadcast({ t: 'drem', id: d.id }); drops.clear(); } log(who, 'turned world drops', state.cfg.drops ? 'on' : 'off'); return { ok: true, on: state.cfg.drops }; }
    case 'resetsave': {
      const nm = String(b.name || '').trim().toLowerCase(), id = byName.get(nm); if (!id) return { ok: false, err: 'No account with that name' };
      if (state.saves.delete(id)) dirtySv.add(id); const a = state.accounts.get(id); if (a) { a.rs = (a.rs | 0) + 1; dirtyAcc = true; } log(who, 'reset the saved progress of', a ? a.name : id); kickAccount(id, 'progress reset'); return { ok: true };
    }
    case 'resetallsaves': {
      if (String(b.confirm || '') !== 'RESET') return { ok: false, err: 'Type RESET to confirm' };
      const n = state.saves.size; for (const id of state.saves.keys()) dirtySv.add(id); state.saves.clear(); log(who, 'RESET ALL PLAYER PROGRESS (' + n + ' saves)'); state.accounts.forEach(a => { a.rs = (a.rs | 0) + 1; }); dirtyAcc = true; [...state.accounts.keys()].forEach(i => kickAccount(i, 'progress reset')); return { ok: true, n };
    }
    case 'setadm': {
      const nm = String(b.name || '').trim().toLowerCase(), id = byName.get(nm), a = id && state.accounts.get(id); if (!a) return { ok: false, err: 'No account with that name' };
      a.adm = !!b.on; dirtyAcc = true; log(who, a.adm ? 'made game admin:' : 'removed game admin:', a.name);
      for (const c of clients.values()) if (c.acc && c.acc.id === a.id) { send(c.ws, { t: 'adm', on: a.adm ? 1 : 0 }); if (a.adm) send(c.ws, adminAdsMsg()); }
      return { ok: true };
    }
    case 'fruitrain': {
      if (!state.cfg.drops) return { ok: false, err: 'Drops are switched off' };
      const live = [...clients.values()]; if (!live.length) return { ok: false, err: 'Nobody is online' };
      for (let i = 0; i < 12; i++) { const c = live[i % live.length], a = Math.random() * 6.283, r = 6 + Math.random() * 40; const k = SEASONS[seasonNow()]; addDrop(Math.max(-WORLD + 5, Math.min(WORLD - 5, c.x + Math.cos(a) * r)), Math.max(-WORLD + 5, Math.min(WORLD - 5, c.z + Math.sin(a) * r)), Math.random() < .15 ? 'box' : k[i % k.length]); }
      pushChat({ sys: true, text: '\ud83c\udf4e Fruit rain! Fruits are falling around the bay.' }, true); log(who, 'started a fruit rain'); return { ok: true };
    }
    case 'adsave': {
      const old = state.ads.find(x => x.id === String(b.ad && b.ad.id || ''));
      if (!old && state.ads.length >= LIM.ads) return { ok: false, err: 'Up to ' + LIM.ads + ' billboards' };
      const src = Object.assign({}, old || {}, b.ad || {}); if (!src.id) src.id = 'ad' + crypto.randomBytes(4).toString('hex');
      if (b.ad && b.ad.img === undefined && old) src.img = old.img;
      const a = cleanAd(src, true); if (!a) return { ok: false, err: 'Check the link (must start with https://), the image (jpg/png/webp under 230 KB) and the position' };
      if (!a.img) return { ok: false, err: 'Add an image' };
      if (old) Object.assign(old, a); else state.ads.push(a);
      dirtyAd = true; pushAds(); log(who, old ? 'edited' : 'created', 'billboard', a.title || a.id); return { ok: true, id: a.id };
    }
    case 'addel': { const i = state.ads.findIndex(x => x.id === String(b.id || '')); if (i < 0) return { ok: false, err: 'No such billboard' }; const a = state.ads.splice(i, 1)[0]; dirtyAd = true; pushAds(); log(who, 'deleted billboard', a.title || a.id); return { ok: true }; }
    case 'say': { const t = cleanText(b.text).slice(0, 140); if (!t) return { ok: false, err: 'Empty message' }; log(who, 'announced:', t); pushChat({ sys: true, text: '[Admin] ' + t }, true); return { ok: true }; }
  }
  return { ok: false, err: 'Unknown action' };
}

initStore().then(() => server.listen(PORT, () => console.log('Sunset Bay server listening on', PORT))).catch(e => { console.error('startup failed', e); process.exit(1); });

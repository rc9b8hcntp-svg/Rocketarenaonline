'use strict';
/* Rocket Arena 3D – serveur de matchmaking (Node >= 18, aucune dépendance).
   - File d'attente par playlist (Casual / Compétitif par MMR) -> salle de match
   - Salons privés entre amis (code à 5 lettres / lien d'invitation)
   - Relais des messages de jeu (entrées, snapshots, buts) entre les joueurs d'une salle
   - Sert aussi le jeu (dossier public/) pour que tout tienne dans un seul service */
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');

const PORT = +process.env.PORT || 8080;
const FILL_S = +process.env.FILL_SECONDS || 20;          // attente avant de compléter avec des bots (hors classé)
const MAX_CONN = +process.env.MAX_CONNECTIONS || 1000;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // vide = tout le monde
const MAXMSG = 32768;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PUB = path.join(__dirname, 'public');

const MODES = { casual: [1, 2, 3], big: [4], ranked: [1, 2, 3], rk_hoops: [2], rk_rumble: [3], rk_drop: [3], rk_snow: [3], rk_heat: [2],
  hoops: [1, 2, 3], rumble: [2, 3], drop: [2, 3], snow: [1, 2, 3], heat: [1, 2, 3] };
const isRanked = k => k === 'ranked' || k.indexOf('rk_') === 0;
const validMode = (k, s) => typeof k === 'string' && MODES[k] && MODES[k].indexOf(s) >= 0;

let nextId = 0;
const conns = new Set();
const queues = new Map();            // "mode|size" -> [{c,mmr,since}]
const rooms = new Set();
const byCode = new Map();

/* ---------- WebSocket minimal (RFC 6455) ---------- */
function parseFrame(b) {
  if (b.length < 2) return null;
  const fin = (b[0] & 128) !== 0, op = b[0] & 15, masked = (b[1] & 128) !== 0;
  let len = b[1] & 127, o = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); o = 4; }
  else if (len === 127) { if (b.length < 10) return null; if (b.readUInt32BE(2)) return { bad: true }; len = b.readUInt32BE(6); o = 10; }
  if (len > MAXMSG) return { bad: true };
  let mask = null;
  if (masked) { if (b.length < o + 4) return null; mask = b.subarray(o, o + 4); o += 4; }
  if (b.length < o + len) return null;
  const p = Buffer.from(b.subarray(o, o + len));
  if (mask) for (let i = 0; i < len; i++) p[i] ^= mask[i & 3];
  return { fin, op, p, n: o + len };
}

class Conn {
  constructor(sock) {
    this.s = sock; this.id = ++nextId; this.buf = Buffer.alloc(0); this.alive = true; this.frag = null;
    this.name = 'Joueur' + this.id; this.L = {}; this.mmr = 500; this.room = null; this.team = 0; this.qkeys = [];
    this.tokens = 400; this.tt = Date.now(); this.lastPong = Date.now();
    sock.setNoDelay(true);
    sock.on('data', d => this.data(d));
    sock.on('close', () => this.close());
    sock.on('error', () => this.close());
  }
  data(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > 262144) return this.kill();
    for (;;) {
      const f = parseFrame(this.buf);
      if (!f) break;
      if (f.bad) return this.kill();
      this.buf = this.buf.subarray(f.n);
      this.frame(f);
      if (!this.alive) return;
    }
  }
  frame(f) {
    if (f.op === 8) { this.kill(); return; }
    if (f.op === 9) { this.write(10, f.p); return; }
    if (f.op === 10) { this.lastPong = Date.now(); return; }
    if (f.op === 1) { if (f.fin) return this.msg(f.p.toString('utf8')); this.frag = [f.p]; return; }
    if (f.op === 0 && this.frag) { this.frag.push(f.p); if (f.fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.msg(s); } }
  }
  write(op, p) {
    if (!this.alive) return;
    const n = p.length; let h;
    if (n < 126) h = Buffer.from([128 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    try { this.s.write(Buffer.concat([h, p])); } catch (e) { return this.kill(); }
    if (this.s.writableLength > 1 << 20) this.kill();
  }
  raw(str) { this.write(1, Buffer.from(str)); }
  send(o) { this.raw(JSON.stringify(o)); }
  err(m) { this.send({ t: 'err', m }); }
  kill() { if (!this.alive) return; try { this.s.destroy(); } catch (e) {} this.close(); }
  close() {
    if (!this.alive && !conns.has(this)) return;
    this.alive = false; conns.delete(this);
    unqueue(this); leaveRoom(this);
  }
  msg(str) {
    const now = Date.now();
    this.tokens = Math.min(400, this.tokens + (now - this.tt) * 0.2); this.tt = now;   // ~200 msg/s soutenus
    if (--this.tokens < 0) return this.kill();
    let m; try { m = JSON.parse(str); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    handle(this, m, str);
  }
}

/* ---------- utilitaires ---------- */
const clean = s => String(s == null ? '' : s).replace(/[\u0000-\u001f<>&"'`]/g, '').trim().slice(0, 16);
function cleanL(L) {
  const o = {};
  if (L && typeof L === 'object') { let n = 0; for (const k of Object.keys(L)) { if (++n > 24) break; const v = L[k]; if (/^[a-z0-9]{1,8}$/i.test(k) && Number.isInteger(v) && v >= 0 && v < 100000) o[k] = v; } }
  return o;
}
function profile(c, m) {
  if (m.name !== undefined) c.name = clean(m.name) || c.name;
  if (m.L !== undefined) c.L = cleanL(m.L);
  if (Number.isFinite(m.mmr)) c.mmr = Math.max(0, Math.min(5000, Math.round(m.mmr)));
}
const CODE_CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  for (;;) { let s = ''; const r = crypto.randomBytes(5); for (let i = 0; i < 5; i++) s += CODE_CH[r[i] % CODE_CH.length]; if (!byCode.has(s)) return s; }
}

/* ---------- file d'attente ---------- */
function unqueue(c) {
  for (const k of c.qkeys) { const L = queues.get(k); if (!L) continue; const i = L.findIndex(e => e.c === c); if (i >= 0) L.splice(i, 1); if (!L.length) queues.delete(k); }
  c.qkeys = [];
}
function enqueue(c, list) {
  unqueue(c);
  const since = Date.now(), seen = new Set();
  for (const it of list.slice(0, 6)) {
    if (!it || !validMode(it.k, it.size)) continue;
    const key = it.k + '|' + it.size; if (seen.has(key)) continue; seen.add(key);
    let L = queues.get(key); if (!L) queues.set(key, L = []);
    L.push({ c, mmr: Number.isFinite(it.mmr) ? Math.max(0, Math.min(5000, it.mmr)) : c.mmr, since });
    c.qkeys.push(key);
  }
  if (!c.qkeys.length) return false;
  qStatus(c); return true;
}
function qStatus(c) {
  const key = c.qkeys[0], L = key && queues.get(key); if (!L) return;
  const me = L.find(e => e.c === c);
  c.send({ t: 'q', n: L.length, on: conns.size, w: me ? Math.floor((Date.now() - me.since) / 1000) : 0 });
}
function tickQueues() {
  const now = Date.now();
  for (const key of [...queues.keys()]) {
    const [k, sz] = key.split('|'), size = +sz, need = size * 2, ranked = isRanked(k);
    for (let guard = 0; guard < 20; guard++) {
      const L = queues.get(key); if (!L || !L.length) break;
      L.sort((a, b) => a.since - b.since);
      const wait = (now - L[0].since) / 1000;
      let group = null;
      if (L.length >= need) {
        if (!ranked) group = L.slice(0, need);
        else {                                             // fenêtre de MMR qui s'élargit avec l'attente
          const win = 120 + 35 * wait, cand = L.filter(e => Math.abs(e.mmr - L[0].mmr) <= win);
          if (cand.length >= need) group = cand.slice(0, need);
        }
      }
      if (!group && !ranked && wait >= FILL_S) group = L.slice(0, need);   // peu de monde : les bots complètent
      if (!group) break;
      startQueueMatch(k, size, group);
    }
  }
  for (const c of conns) if (c.qkeys.length) qStatus(c);
}
function startQueueMatch(k, size, group) {
  const players = group.map(e => e.c);
  for (const c of players) unqueue(c);
  const room = { players: new Map(), host: 0, priv: false, state: 'play', cfg: { mode: k, size, dur: isRanked(k) ? 5 : 3 }, code: null };
  rooms.add(room);
  const sorted = group.slice().sort((a, b) => b.mmr - a.mmr), cnt = [0, 0], seq = [0, 1, 1, 0, 0, 1, 1, 0];
  sorted.forEach((e, i) => { let t = seq[i % 8]; if (cnt[t] >= size) t = 1 - t; cnt[t]++; e.c.team = t; });
  for (const c of players) { c.room = room; room.players.set(c.id, c); }
  room.host = players[0].id;
  launch(room);
}
function launch(room) {
  room.state = 'play';
  const slot = [0, 0], list = [];
  for (const c of room.players.values()) list.push({ id: c.id, name: c.name, team: c.team, slot: slot[c.team]++, L: c.L, mmr: c.mmr });
  for (const c of room.players.values()) c.send({ t: 'match', you: c.id, host: room.host, cfg: room.cfg, players: list, priv: room.priv });
}

/* ---------- salons ---------- */
function roomState(room) {
  return { t: 'room', code: room.code, host: room.host, state: room.state, cfg: room.cfg,
    players: [...room.players.values()].map(c => ({ id: c.id, name: c.name, team: c.team, mmr: c.mmr })) };
}
function bcastRoom(room) { const s = JSON.stringify(roomState(room)); for (const c of room.players.values()) c.raw(s); }
function leaveRoom(c) {
  const room = c.room; if (!room) return;
  c.room = null; room.players.delete(c.id);
  if (!room.players.size) { rooms.delete(room); if (room.code) byCode.delete(room.code); return; }
  if (room.host === c.id) room.host = room.players.keys().next().value;
  if (room.state === 'play') { const s = JSON.stringify({ t: 'left', id: c.id, host: room.host }); for (const o of room.players.values()) o.raw(s); }
  else bcastRoom(room);
}
function rebalance(room) {                                  // redistribue les équipes selon la taille du format
  const size = room.cfg.size, cnt = [0, 0];
  for (const c of room.players.values()) { if (cnt[c.team] < size) cnt[c.team]++; else { c.team = 1 - c.team; cnt[c.team]++; } }
}

/* ---------- messages ---------- */
const RELAY_ALL = { in: 1 }, RELAY_HOST = { snap: 1, goal: 1, kick: 1 };
function handle(c, m, str) {
  const t = m.t;
  if (RELAY_ALL[t] || RELAY_HOST[t]) {
    const r = c.room; if (!r || r.state !== 'play') return;
    if (RELAY_HOST[t] && c.id !== r.host) return;
    for (const o of r.players.values()) if (o !== c) o.raw(str);
    return;
  }
  switch (t) {
    case 'hello': profile(c, m); c.send({ t: 'welcome', id: c.id, on: conns.size }); break;
    case 'ping': c.send({ t: 'pong', ts: m.ts }); break;
    case 'queue': {
      profile(c, m);
      if (c.room) { if (c.room.state === 'play' && c.room.players.size > 1 && !c.room.over) return c.err('Déjà en match'); leaveRoom(c); }
      if (!Array.isArray(m.modes) || !enqueue(c, m.modes)) c.err('Playlist invalide');
      break;
    }
    case 'unqueue': unqueue(c); break;
    case 'create': {
      profile(c, m); unqueue(c); leaveRoom(c);
      const room = { players: new Map(), host: c.id, priv: true, state: 'lobby', cfg: { mode: 'casual', size: 2, dur: 3 }, code: newCode() };
      rooms.add(room); byCode.set(room.code, room);
      c.room = room; c.team = 0; room.players.set(c.id, c); bcastRoom(room); break;
    }
    case 'join': {
      profile(c, m); unqueue(c);
      const room = byCode.get(String(m.code || '').toUpperCase().trim());
      if (!room) return c.err('Salon introuvable');
      if (room.state !== 'lobby') return c.err('Match déjà en cours');
      if (c.room === room) return bcastRoom(room);
      if (room.players.size >= room.cfg.size * 2) return c.err('Salon complet');
      leaveRoom(c);
      const cnt = [0, 0]; for (const o of room.players.values()) cnt[o.team]++;
      c.team = cnt[0] <= cnt[1] ? 0 : 1; c.room = room; room.players.set(c.id, c); bcastRoom(room); break;
    }
    case 'team': {
      const r = c.room; if (!r || r.state !== 'lobby') return;
      const tm = m.team === 1 ? 1 : 0; let n = 0; for (const o of r.players.values()) if (o.team === tm && o !== c) n++;
      if (n >= r.cfg.size) return c.err('Équipe complète');
      c.team = tm; bcastRoom(r); break;
    }
    case 'cfg': {
      const r = c.room; if (!r || r.state !== 'lobby' || r.host !== c.id) return;
      if (!validMode(m.mode, m.size) || isRanked(m.mode)) return c.err('Format invalide');
      if (r.players.size > m.size * 2) return c.err('Trop de joueurs pour ce format');
      r.cfg = { mode: m.mode, size: m.size, dur: [3, 5, 10].indexOf(m.dur) >= 0 ? m.dur : 3 };
      rebalance(r); bcastRoom(r); break;
    }
    case 'start': {
      const r = c.room; if (!r || r.state !== 'lobby' || r.host !== c.id) return;
      launch(r); break;
    }
    case 'end': {
      const r = c.room; if (!r || r.state !== 'play' || r.host !== c.id) return;
      for (const o of r.players.values()) if (o !== c) o.raw(str);
      if (r.priv) { r.state = 'lobby'; bcastRoom(r); } else r.over = true;
      break;
    }
    case 'leave': unqueue(c); leaveRoom(c); break;
  }
}

/* ---------- HTTP + upgrade ---------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  let p; try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (p === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    const q = {}; for (const [k, L] of queues) q[k] = L.length;
    return res.end(JSON.stringify({ online: conns.size, rooms: rooms.size, queues: q }));
  }
  if (p === '/') p = '/index.html';
  const f = path.join(PUB, path.normalize(p));
  if (f.indexOf(PUB) !== 0) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Rocket Arena matchmaking server – OK (mets le jeu dans public/index.html pour le servir ici)'); }
    const ext = path.extname(f).toLowerCase();
    res.writeHead(200, { 'Content-Type': TYPES[ext] || 'application/octet-stream', 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600' });
    res.end(d);
  });
});
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'], origin = req.headers.origin;
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || conns.size >= MAX_CONN ||
      (ORIGINS.length && origin && ORIGINS.indexOf(origin) < 0)) { sock.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return sock.destroy(); }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  conns.add(new Conn(sock));
});

setInterval(tickQueues, 1000);
setInterval(() => {                                          // keep-alive + nettoyage des connexions mortes
  const now = Date.now();
  for (const c of [...conns]) { if (now - c.lastPong > 60000) c.kill(); else c.write(9, Buffer.alloc(0)); }
}, 20000);

if (require.main === module) server.listen(PORT, () => console.log('Matchmaking Rocket Arena sur le port ' + PORT));
module.exports = { server, tickQueues };

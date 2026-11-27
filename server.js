'use strict';
/* Rocket Arena 3D – serveur de matchmaking (Node >= 18, aucune dépendance).
   - File d'attente par playlist (Casual / Compétitif par MMR) -> salle de match ; un groupe entier peut s'inscrire (même équipe)
   - Comptes (jeton Firebase vérifié) : vraie liste d'amis (code ami, demandes, présence en ligne / en match…)
   - Groupes (4 joueurs max) : invitations entre amis ; les matchs privés ne se lancent QU'À PLUSIEURS, depuis un groupe
   - Relais des messages de jeu (entrées, snapshots, buts) entre les joueurs d'une salle
   - Sert aussi le jeu (dossier public/) pour que tout tienne dans un seul service */
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path'), zlib = require('zlib');

const PORT = +process.env.PORT || 8080;
const FILL_S = +process.env.FILL_SECONDS || 20;          // attente avant de compléter avec des bots (hors classé)
const MAX_CONN = +process.env.MAX_CONNECTIONS || 1000;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // vide = tout le monde
const GRACE = +process.env.GRACE_SECONDS || 20;          // après une coupure réseau, la place du joueur est gardée pendant ce délai
const MAXMSG = 32768;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PUB = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');      // comptes / amis (mettre un disque persistant en production)
const ACC_FILE = path.join(DATA_DIR, 'accounts.json');
const FB_PROJECT = process.env.FIREBASE_PROJECT_ID || 'rocketarena';        // projet Firebase du jeu (vérification des jetons de connexion)
const MAP_LIST = ['champ', 'mann', 'neo', 'utopia', 'waste', 'urban', 'beck', 'forb', 'salty', 'aqua', 'farm'];   // ambiances de map (le client connaît les mêmes clés)
const randomMap = () => MAP_LIST[crypto.randomInt(MAP_LIST.length)];
const PARTY_MAX = 4, MAX_FRIENDS = 100, MAX_REQ = 50, INVITE_S = 60;

const logErr = (where, e) => console.error('[' + new Date().toISOString() + '] ' + where + ' :', (e && e.stack) || e);

const MODES = { casual: [1, 2, 3], big: [4], ranked: [1, 2, 3], rk_hoops: [2], rk_rumble: [3], rk_drop: [3], rk_snow: [3], rk_heat: [2],
  hoops: [1, 2, 3], rumble: [2, 3], drop: [2, 3], snow: [1, 2, 3], heat: [1, 2, 3] };
const isRanked = k => k === 'ranked' || k.indexOf('rk_') === 0;
/* hasOwnProperty : sans ça, k = "constructor" / "__proto__" faisait planter tout le serveur (MODES[k].indexOf n'existe pas) */
const validMode = (k, s) => typeof k === 'string' && Object.prototype.hasOwnProperty.call(MODES, k) && MODES[k].indexOf(s) >= 0;

let nextId = 0;
const conns = new Set();
const queues = new Map();            // "mode|size" -> [{c,mmr,since}]
const rooms = new Set();
const accounts = new Map();          // uid -> { uid, name, code, friends:[uid], inc:[uid], out:[uid] }
const byFCode = new Map();           // code ami -> compte
const online = new Map();            // uid -> Conn connectée à ce compte
const parties = new Set();
let nextParty = 0;
const ghosts = new Map();            // id -> Conn dont le socket est coupé mais dont la place (salon / match) est gardée GRACE secondes

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
    this.tok = crypto.randomBytes(8).toString('hex'); this.bye = false; this.ghostT = null; this.moved = false;
    this.uid = null; this.acct = null; this.party = null; this.ptm = 0; this.lastSt = null;   // tok : jeton de reprise de session
    sock.setNoDelay(true); sock.setKeepAlive(true, 10000); sock.setTimeout(0);
    sock.on('data', d => { try { this.data(d); } catch (e) { logErr('data', e); this.kill(); } });
    /* Les sockets HTTP de Node sont en « half-open » : quand le client coupe, 'close' n'arrive pas tout de suite
       (parfois jamais avant 60 s) et le joueur restait fantôme dans le match. 'end' = le client a coupé : on le traite tout de suite. */
    sock.on('end', () => this.close());
    sock.on('close', () => this.close());
    sock.on('error', () => this.close());
  }
  data(d) {
    if (!this.alive) return;
    this.lastPong = Date.now();                              // n'importe quel octet reçu prouve que le client est vivant
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
    if (f.op === 1) { if (f.fin) return this.msg(f.p.toString('utf8')); this.frag = [f.p]; this.fragN = f.p.length; return; }
    if (f.op === 0 && this.frag) {
      this.frag.push(f.p); this.fragN += f.p.length;
      if (this.fragN > 4 * MAXMSG) return this.kill();       // message fragmenté sans fin : on coupe (mémoire)
      if (f.fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.msg(s); }
    }
  }
  write(op, p) {
    if (!this.alive) return;
    const n = p.length; let h;
    if (n < 126) h = Buffer.from([128 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    try { this.s.write(Buffer.concat([h, p])); } catch (e) { return this.drop(); }
    if (this.s.writableLength > 1 << 20) this.drop();
  }
  raw(str) { this.write(1, Buffer.from(str)); }
  send(o) { this.raw(JSON.stringify(o)); }
  err(m) { this.send({ t: 'err', m }); }
  drop() { if (!this.alive) return; try { this.s.destroy(); } catch (e) {} this.close(); }   // connexion morte / bloquée : on garde la place GRACE secondes
  kill() { if (!this.alive) return; this.bye = true; try { this.s.destroy(); } catch (e) {} this.close(); }
  close() {
    if (this.ghostT || this.moved) return;
    if (!this.alive && !conns.has(this)) return;
    try { this.s.destroy(); } catch (e) {}                   // le socket est mort ou à moitié fermé : on le libère toujours (sinon fuite de sockets)
    /* coupure réseau (pas un départ volontaire, pas un kill du serveur) pendant un salon / un match :
       on garde la place GRACE secondes, le client peut se reconnecter et reprendre exactement où il en était */
    if (this.alive && !this.bye && (this.room || this.party)) {
      this.alive = false; ghosts.set(this.id, this);
      this.ghostT = setTimeout(() => this.fin(), GRACE * 1000);
      return;
    }
    this.fin();
  }
  fin() {
    this.alive = false; clearTimeout(this.ghostT); this.ghostT = null; ghosts.delete(this.id); conns.delete(this);
    const u = this.uid, a = this.acct;
    unqueue(this); leaveRoom(this); leaveParty(this);
    if (u && online.get(u) === this) {
      online.delete(u);
      if (a) for (const f of a.friends) { const o = online.get(f); if (o) o.send({ t: 'fr_st', uid: u, st: 'off' }); }
    }
  }
  msg(str) {
    const now = Date.now();
    this.tokens = Math.min(400, this.tokens + (now - this.tt) * 0.2); this.tt = now;   // ~200 msg/s soutenus
    if (--this.tokens < -400) return this.kill();            // flood massif : on coupe
    if (this.tokens < 0) return;                             // petit pic (onglet qui reprend, lag) : on ignore le message au lieu de virer le joueur
    let m; try { m = JSON.parse(str); } catch (e) { return; }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
    try { handle(this, m, str); } catch (e) { logErr('handle:' + m.t, e); }   // un message mal formé ne doit jamais faire tomber le serveur
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
function newFCode() {                                       // code ami à 8 caractères (affiché XXXX-XXXX)
  for (;;) { let s = ''; const r = crypto.randomBytes(8); for (let i = 0; i < 8; i++) s += CODE_CH[r[i] % CODE_CH.length]; if (!byFCode.has(s)) return s; }
}
const fmtCode = c => c.slice(0, 4) + '-' + c.slice(4);

/* ---------- comptes (sauvegarde sur disque) ---------- */
function loadAccounts() {
  try {
    const d = JSON.parse(fs.readFileSync(ACC_FILE, 'utf8'));
    for (const a of d.accounts || []) { if (!a || !a.uid || !a.code) continue; a.friends = a.friends || []; a.inc = a.inc || []; a.out = a.out || []; accounts.set(a.uid, a); byFCode.set(a.code, a); }
  } catch (e) {
    if (e.code !== 'ENOENT') {
      console.error('Lecture des comptes impossible :', e.message);
      /* fichier corrompu : on le met de côté AVANT qu'une sauvegarde ne l'écrase par une liste vide (perte de tous les amis) */
      try { fs.renameSync(ACC_FILE, ACC_FILE + '.corrupt-' + Date.now()); } catch (e2) {}
    }
  }
}
let saveT = null;
function saveAccounts() {
  if (saveT) return;
  saveT = setTimeout(saveNow, 1500);
}
function saveNow() {
  clearTimeout(saveT); saveT = null;
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = ACC_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ v: 1, accounts: [...accounts.values()] }));
    try { fs.copyFileSync(ACC_FILE, ACC_FILE + '.bak'); } catch (e) {}   // dernière version saine conservée
    fs.renameSync(tmp, ACC_FILE);
  } catch (e) { console.error('Sauvegarde des comptes impossible :', e.message); }
}

/* ---------- vérification du jeton Firebase (RS256, certificats Google) ---------- */
const CERT_URL = 'https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com';
let certs = null, certsExp = 0, certsP = null, certOverride = null;
async function getCerts(force) {
  if (certOverride) return certOverride();
  if (!force && certs && Date.now() < certsExp) return certs;
  if (certsP) return certsP;
  certsP = (async () => {
    try {
      /* timeout : sans lui, un Google qui ne répond pas laissait TOUTES les connexions de compte en attente indéfiniment */
      const r = await fetch(CERT_URL, { signal: AbortSignal.timeout(8000) }); if (!r.ok) throw new Error('certificats ' + r.status);
      const m = /max-age=(\d+)/.exec(r.headers.get('cache-control') || '');
      certs = await r.json(); certsExp = Date.now() + (m ? +m[1] : 3600) * 1000; return certs;
    } catch (e) {
      logErr('certificats Google', e);
      if (certs) { certsExp = Date.now() + 60000; return certs; }   // panne passagère : on garde les anciens certificats (déjà vérifiés) et on réessaie dans 1 min
      throw e;
    }
  })().finally(() => { certsP = null; });
  return certsP;
}
const b64u = s => Buffer.from(String(s).replace(/-/g, '+').replace(/_/g, '/'), 'base64');
async function verifyIdToken(tok) {
  const p = String(tok || '').split('.'); if (p.length !== 3) throw new Error('format');
  const h = JSON.parse(b64u(p[0]).toString()), pl = JSON.parse(b64u(p[1]).toString());
  if (h.alg !== 'RS256' || !h.kid) throw new Error('algorithme');
  let cs = await getCerts(false), pem = cs[h.kid];
  if (!pem) { cs = await getCerts(true); pem = cs[h.kid]; }          // les clés tournent : on recharge une fois
  if (!pem) throw new Error('clé inconnue');
  const key = new crypto.X509Certificate(pem).publicKey;
  if (!crypto.verify('RSA-SHA256', Buffer.from(p[0] + '.' + p[1]), key, b64u(p[2]))) throw new Error('signature');
  const now = Math.floor(Date.now() / 1000);
  if (pl.aud !== FB_PROJECT || pl.iss !== 'https://securetoken.google.com/' + FB_PROJECT || !pl.sub || !(pl.exp > now) || pl.iat > now + 300) throw new Error('jeton refusé');
  return { uid: String(pl.sub) };
}

/* ---------- file d'attente : joueurs seuls ou groupes entiers (un groupe reste dans la même équipe) ---------- */
function packTeams(entries, size) {                         // répartit des entrées (n joueurs chacune) dans 2 équipes de `size` places
  const es = entries.slice().sort((a, b) => b.n - a.n), cap = [size, size], asg = new Map();
  const go = i => {
    if (i === es.length) return true;
    for (let t = 0; t < 2; t++) if (cap[t] >= es[i].n) { cap[t] -= es[i].n; asg.set(es[i], t); if (go(i + 1)) return true; cap[t] += es[i].n; }
    return false;
  };
  return go(0) ? asg : null;
}
function unqueue(c) {
  if (!c.qkeys.length) return;
  const grp = new Set([c]);
  for (const k of c.qkeys) { const L = queues.get(k); if (L) for (const e of L) if (e.cs.indexOf(c) >= 0) for (const x of e.cs) grp.add(x); }
  for (const m of grp) {
    for (const k of m.qkeys) { const L = queues.get(k); if (!L) continue; for (let i = L.length - 1; i >= 0; i--) if (L[i].cs.indexOf(m) >= 0) L.splice(i, 1); if (!L.length) queues.delete(k); }
    m.qkeys = [];
  }
  if (grp.size > 1) { for (const m of grp) m.send({ t: 'p_q', on: 0 }); if (c.party) partyBcast(c.party); }
}
const qerr = (c, m) => c.send({ t: 'err', m, q: 1 });
function busy(m) { return !!(m.room && m.room.state === 'play' && !m.room.over); }
function enqueue(c, list) {
  unqueue(c);
  const p = c.party, grp = p && p.members.size >= 2 ? [...p.members.values()] : [c];
  for (const m of grp) if (busy(m)) { qerr(c, m === c ? 'Déjà en match' : m.name + ' est déjà en match'); return false; }
  for (const m of grp) if (!m.alive) { qerr(c, m.name + ' est déconnecté'); return false; }
  const since = Date.now(), seen = new Set(); let tooBig = false;
  for (const it of list.slice(0, 6)) {
    if (!it || !validMode(it.k, it.size)) continue;
    if (it.size < grp.length) { tooBig = true; continue; }
    const key = it.k + '|' + it.size; if (seen.has(key)) continue; seen.add(key);
    let L = queues.get(key); if (!L) queues.set(key, L = []);
    const mm = grp.map(m => m === c ? (Number.isFinite(it.mmr) ? Math.max(0, Math.min(5000, it.mmr)) : c.mmr) : m.mmr);
    L.push({ cs: grp, n: grp.length, mmr: mm.reduce((a, b) => a + b, 0) / mm.length, since });
    for (const m of grp) m.qkeys.push(key);
  }
  if (!c.qkeys.length) { qerr(c, tooBig ? 'Ton groupe est trop grand pour cette playlist' : 'Playlist invalide'); return false; }
  if (grp.length > 1) { for (const m of grp) if (m !== c) m.send({ t: 'p_q', on: 1 }); if (p) partyBcast(p); }
  for (const m of grp) qStatus(m);
  return true;
}
function qStatus(c) {
  const key = c.qkeys[0], L = key && queues.get(key); if (!L) return;
  const me = L.find(e => e.cs.indexOf(c) >= 0);
  c.send({ t: 'q', n: L.reduce((s, e) => s + e.n, 0), on: conns.size, w: me ? Math.floor((Date.now() - me.since) / 1000) : 0 });
}
function pickGroup(cands, need, size) {                       // sous-ensemble contenant la plus ancienne entrée, exactement `need` joueurs, répartissable en 2 équipes
  const C = cands.slice(0, 14); let best = null;
  (function dfs(i, chosen, tot) {
    if (best) return;
    if (tot === need) { if (packTeams(chosen, size)) best = chosen.slice(); return; }
    for (let j = i; j < C.length && !best; j++) { const n = C[j].n; if (tot + n <= need) { chosen.push(C[j]); dfs(j + 1, chosen, tot + n); chosen.pop(); } }
  })(1, [C[0]], C[0].n);
  return best;
}
function fillGroup(cands, need, size) {                       // peu de monde : on prend ce qu'on peut, les bots complètent
  const chosen = [cands[0]]; let tot = cands[0].n;
  for (let j = 1; j < cands.length; j++) { const e = cands[j]; if (tot + e.n <= need && packTeams(chosen.concat(e), size)) { chosen.push(e); tot += e.n; } }
  return chosen;
}
function tickQueues() {
  const now = Date.now();
  for (const key of [...queues.keys()]) {
    const [k, sz] = key.split('|'), size = +sz, need = size * 2, ranked = isRanked(k);
    for (let guard = 0; guard < 20; guard++) {
      const L = queues.get(key); if (!L || !L.length) break;
      L.sort((a, b) => a.since - b.since);
      const wait = (now - L[0].since) / 1000;
      const cands = ranked ? L.filter(e => e === L[0] || Math.abs(e.mmr - L[0].mmr) <= 120 + 35 * wait) : L;   // fenêtre de MMR qui s'élargit avec l'attente
      let group = pickGroup(cands, need, size);
      if (!group && !ranked && wait >= FILL_S) group = fillGroup(cands, need, size);   // le classé n'est jamais complété
      if (!group) break;
      startQueueMatch(k, size, group);
    }
  }
  for (const c of conns) if (c.qkeys.length) qStatus(c);
}
function startQueueMatch(k, size, group) {
  const players = [];
  for (const e of group) for (const c of e.cs) if (players.indexOf(c) < 0) players.push(c);
  for (const c of players) { unqueue(c); if (c.room) leaveRoom(c); }   // un coéquipier de groupe pouvait rester inscrit dans son ancien match et recevoir ses messages
  const room = { players: new Map(), host: 0, priv: false, state: 'play', cfg: { mode: k, size, dur: isRanked(k) ? 5 : 3 }, code: null, party: null, map: randomMap() };   // la map est tirée au sort par le serveur : la même pour tous les joueurs
  rooms.add(room);
  const cnt = [0, 0], mm = [0, 0], pe = group.filter(e => e.n > 1), asg = packTeams(pe, size) || new Map();
  for (const e of pe) { const t = asg.get(e) || 0; cnt[t] += e.n; for (const c of e.cs) { c.team = t; mm[t] += c.mmr; } }
  const solos = group.filter(e => e.n === 1).map(e => e.cs[0]).sort((a, b) => b.mmr - a.mmr);
  for (const c of solos) { const t = cnt[0] >= size ? 1 : cnt[1] >= size ? 0 : (mm[0] <= mm[1] ? 0 : 1); c.team = t; cnt[t]++; mm[t] += c.mmr; }
  for (const c of players) { c.room = room; room.players.set(c.id, c); }
  room.host = players[0].id;
  launch(room);
  refreshParties(room);
}
function matchMsg(room, c) {
  const slot = [0, 0], list = [];
  for (const o of room.players.values()) list.push({ id: o.id, name: o.name, team: o.team, slot: slot[o.team]++, L: o.L, mmr: o.mmr });
  return { t: 'match', you: c.id, host: room.host, cfg: room.cfg, players: list, priv: room.priv, map: room.map };
}
function launch(room) {
  room.state = 'play';
  for (const c of room.players.values()) c.send(matchMsg(room, c));
}
/* reprise de session : la nouvelle connexion c reprend la place de la connexion coupée old */
function adopt(old, c) {
  clearTimeout(old.ghostT); old.ghostT = null; old.moved = true; ghosts.delete(old.id); conns.delete(old);
  c.id = old.id; c.tok = old.tok; c.room = old.room; c.team = old.team; c.ptm = old.ptm;
  if (old.qkeys.length) {                                   // recherche de match en cours : la file doit pointer vers la NOUVELLE connexion (sinon le match démarrait avec un joueur mort)
    for (const k of old.qkeys) { const L = queues.get(k); if (L) for (const e of L) { const i = e.cs.indexOf(old); if (i >= 0) e.cs[i] = c; } }
    c.qkeys = old.qkeys; old.qkeys = [];
  }
  if (c.room) c.room.players.set(c.id, c);
  c.party = old.party; if (c.party) c.party.members.set(c.id, c);
  c.uid = old.uid; c.acct = old.acct; if (c.uid) online.set(c.uid, c);
  old.room = null; old.party = null; old.uid = null; old.acct = null;
}

/* ---------- salles de match ---------- */
function leaveRoom(c) {
  const room = c.room; if (!room) return;
  c.room = null; room.players.delete(c.id);
  if (!room.players.size) { rooms.delete(room); if (room.party) { room.party.state = 'lobby'; partyBcast(room.party); } if (c.party) partyBcast(c.party); return; }
  if (room.host === c.id) {                                  // nouvel hôte : de préférence un joueur réellement connecté
    let h = null; for (const o of room.players.values()) { if (o.alive) { h = o; break; } }
    room.host = h ? h.id : room.players.keys().next().value;
  }
  const s = JSON.stringify({ t: 'left', id: c.id, host: room.host }); for (const o of room.players.values()) o.raw(s);
  if (c.party) partyBcast(c.party);
}
function rebalance(room) {                                  // redistribue les équipes selon la taille du format
  const size = room.cfg.size, cnt = [0, 0];
  for (const c of room.players.values()) { if (cnt[c.team] < size) cnt[c.team]++; else { c.team = 1 - c.team; cnt[c.team]++; } }
}

/* ---------- amis ---------- */
const nameOf = u => { const a = accounts.get(u); return a ? a.name : '?'; };
function stOf(c) {                                           // statut visible par les amis
  if (!c) return 'off';
  if (busy(c)) return 'match';
  if (c.qkeys.length) return 'queue';
  if (c.party && c.party.members.size >= 2) return 'party';
  return 'online';
}
function sendFriends(c) {
  const a = c.acct; if (!a) return;
  c.send({ t: 'fr_list', code: fmtCode(a.code),
    friends: a.friends.map(u => ({ uid: u, name: nameOf(u), st: stOf(online.get(u)) })),
    inc: a.inc.map(u => ({ uid: u, name: nameOf(u) })), out: a.out.map(u => ({ uid: u, name: nameOf(u) })) });
}
function pushSt(c, st) { if (!c.acct) return; for (const f of c.acct.friends) { const o = online.get(f); if (o) o.send({ t: 'fr_st', uid: c.uid, st }); } }
function needAcct(c) { if (c.acct) return c.acct; c.err('Connecte-toi à ton compte pour utiliser les amis et le groupe'); return null; }
function frRefresh(uid) { const o = online.get(uid); if (o) sendFriends(o); }
function frAdd(c, raw) {
  const a = needAcct(c); if (!a) return;
  const code = String(raw || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const b = byFCode.get(code); if (!b) return c.err('Code ami introuvable');
  if (b.uid === a.uid) return c.err('C\'est ton propre code ami');
  if (a.friends.indexOf(b.uid) >= 0) return c.err('Tu es déjà ami avec ' + b.name);
  if (a.out.indexOf(b.uid) >= 0) return c.err('Demande déjà envoyée à ' + b.name);
  if (a.inc.indexOf(b.uid) >= 0) return frAccept(c, b.uid);   // il t'avait déjà invité : vous devenez amis
  if (a.friends.length >= MAX_FRIENDS) return c.err('Ta liste d\'amis est pleine');
  if (b.inc.length >= MAX_REQ || b.friends.length >= MAX_FRIENDS) return c.err('Ce joueur ne peut plus recevoir de demandes');
  a.out.push(b.uid); b.inc.push(a.uid); saveAccounts();
  c.send({ t: 'fr_ok', m: 'Demande envoyée à ' + b.name }); sendFriends(c);
  const bc = online.get(b.uid); if (bc) { bc.send({ t: 'fr_req', uid: a.uid, name: a.name }); sendFriends(bc); }
}
function frAccept(c, uid) {
  const a = needAcct(c); if (!a) return;
  const i = a.inc.indexOf(uid), b = accounts.get(uid); if (i < 0 || !b) return c.err('Demande introuvable');
  a.inc.splice(i, 1); const j = b.out.indexOf(a.uid); if (j >= 0) b.out.splice(j, 1);
  if (a.friends.indexOf(uid) < 0) a.friends.push(uid); if (b.friends.indexOf(a.uid) < 0) b.friends.push(a.uid);
  saveAccounts(); c.send({ t: 'fr_ok', m: 'Tu es maintenant ami avec ' + b.name });
  sendFriends(c); const bc = online.get(uid); if (bc) { bc.send({ t: 'fr_ok', m: a.name + ' a accepté ta demande' }); sendFriends(bc); }
}
function frDrop(c, uid, kind) {                              // kind : decline (refuser), cancel (annuler), remove (supprimer un ami)
  const a = needAcct(c); if (!a) return;
  const b = accounts.get(uid); const rm = (arr, x) => { const i = arr.indexOf(x); if (i >= 0) arr.splice(i, 1); };
  if (kind === 'decline') { rm(a.inc, uid); if (b) rm(b.out, uid); }
  else if (kind === 'cancel') { rm(a.out, uid); if (b) rm(b.inc, uid); }
  else { rm(a.friends, uid); if (b) rm(b.friends, a.uid); }
  saveAccounts(); sendFriends(c); frRefresh(uid);
}

/* ---------- compte : liaison jeton Firebase <-> connexion ---------- */
function bindAcct(c, uid, name) {
  let a = accounts.get(uid); const nm = clean(name);
  if (!a) { a = { uid, name: nm || 'Joueur', code: newFCode(), friends: [], inc: [], out: [], created: Date.now() }; accounts.set(uid, a); byFCode.set(a.code, a); saveAccounts(); }
  else if (nm && nm !== a.name) { a.name = nm; saveAccounts(); }
  const old = online.get(uid);
  if (old && old !== c) { old.send({ t: 'err', m: 'Compte connecté depuis un autre appareil' }); if (old.ghostT) old.fin(); else old.kill(); }
  if (c.uid && c.uid !== uid) unbind(c, true);
  c.uid = uid; c.acct = a; online.set(uid, c);
  c.send({ t: 'acct', uid, code: fmtCode(a.code), name: a.name });
  sendFriends(c); if (c.party) partyBcast(c.party);
  c.lastSt = stOf(c); pushSt(c, c.lastSt);
}
function unbind(c, silent) {
  const u = c.uid, a = c.acct; if (!u) return;
  leaveParty(c);
  if (online.get(u) === c) online.delete(u);
  if (a) for (const f of a.friends) { const o = online.get(f); if (o) o.send({ t: 'fr_st', uid: u, st: 'off' }); }
  c.uid = null; c.acct = null; c.lastSt = null;
  if (!silent) c.send({ t: 'acct', uid: null });
}
function doAuth(c, m) {
  profile(c, m);
  if (c.authing) return;                                     // une vérification est déjà en cours pour cette connexion
  c.authing = true;
  verifyIdToken(m.auth).then(u => {
    c.authing = false;
    if (c.alive) { try { bindAcct(c, u.uid, m.name); } catch (e) { logErr('bindAcct', e); c.send({ t: 'acct', uid: null, err: 1 }); } }
  }, e => {
    c.authing = false;
    if (!/format|algorithme|signature|jeton refusé|clé inconnue/.test(String(e && e.message))) logErr('auth', e);   // erreurs réseau seulement (un jeton invalide est normal)
    c.send({ t: 'acct', uid: null, err: 1 });
  });
}

/* ---------- groupes ---------- */
function partyView(p) {
  const ms = [...p.members.values()], lead = p.members.get(p.leader);
  let state = 'lobby';
  if (p.state === 'match' || ms.some(busy)) state = 'match'; else if (lead && lead.qkeys.length) state = 'queue';
  return { id: p.id, leader: p.leader, state, cfg: p.cfg,
    members: ms.map(m => ({ id: m.id, uid: m.uid, name: m.acct ? m.acct.name : m.name, team: m.ptm, mmr: m.mmr, L: m.L || {} })),
    inv: [...p.inv.keys()].map(u => ({ uid: u, name: nameOf(u) })) };
}
function partyBcast(p) { const s = JSON.stringify({ t: 'party', party: partyView(p) }); for (const m of p.members.values()) m.raw(s); }
function refreshParties(room) { const seen = new Set(); for (const c of room.players.values()) if (c.party && !seen.has(c.party)) { seen.add(c.party); partyBcast(c.party); } }
function dissolve(p) { for (const m of p.members.values()) { m.party = null; m.send({ t: 'party', party: null }); } p.members.clear(); parties.delete(p); }
function pickTeam(p) { const cnt = [0, 0]; for (const m of p.members.values()) cnt[m.ptm]++; return cnt[0] <= cnt[1] ? 0 : 1; }
function rebalanceParty(p) { const size = p.cfg.size, cnt = [0, 0]; for (const c of p.members.values()) { if (cnt[c.ptm] < size) cnt[c.ptm]++; else { c.ptm = 1 - c.ptm; cnt[c.ptm]++; } } }
function leaveParty(c) {
  const p = c.party; if (!p) return;
  if (c.qkeys.length) unqueue(c);
  c.party = null; p.members.delete(c.id);
  c.send({ t: 'party', party: null });
  if (p.leader === c.id && p.members.size) p.leader = p.members.keys().next().value;
  if (p.members.size <= 1 && !p.inv.size) dissolve(p); else partyBcast(p);
}
function pInvite(c, uid) {
  const a = needAcct(c); if (!a) return;
  if (a.friends.indexOf(uid) < 0) return c.err('Ce joueur n\'est pas dans ta liste d\'amis');
  const t = online.get(uid); if (!t || !t.alive) return c.err(nameOf(uid) + ' est hors ligne');
  if (busy(t)) return c.err(nameOf(uid) + ' est en match');
  let p = c.party;
  if (p && p.state !== 'lobby') return c.err('Match en cours : invite après la partie');
  if (p && [...p.members.values()].some(m => m.uid === uid)) return c.err(nameOf(uid) + ' est déjà dans le groupe');
  if (p && p.members.size + p.inv.size >= PARTY_MAX) return c.err('Le groupe est complet (' + PARTY_MAX + ' joueurs)');
  if (!p) { p = { id: 'P' + (++nextParty), leader: c.id, members: new Map(), cfg: { mode: 'casual', size: 2, dur: 3, map: 'rand' }, inv: new Map(), state: 'lobby' }; p.members.set(c.id, c); c.party = p; c.ptm = 0; parties.add(p); }
  p.inv.set(uid, Date.now() + INVITE_S * 1000);
  t.send({ t: 'p_inv', pid: p.id, uid: a.uid, name: a.name, ttl: INVITE_S });
  c.send({ t: 'fr_ok', m: 'Invitation envoyée à ' + nameOf(uid) });
  partyBcast(p);
}
function findParty(pid) { for (const p of parties) if (p.id === pid) return p; return null; }
function pAccept(c, pid) {
  if (!needAcct(c)) return;
  const p = findParty(pid), exp = p && p.inv.get(c.uid);
  if (!p || !exp || exp < Date.now()) { if (p) { p.inv.delete(c.uid); } return c.err('Invitation expirée'); }
  if (p.state !== 'lobby') return c.err('Un match est déjà en cours dans ce groupe');
  if (p.members.size >= PARTY_MAX) return c.err('Le groupe est complet');
  if (c.party === p) return;
  if (c.party) leaveParty(c); else if (c.qkeys.length) unqueue(c);
  p.inv.delete(c.uid); c.party = p; c.ptm = pickTeam(p); p.members.set(c.id, c);
  rebalanceParty(p); partyBcast(p);
}
function pDecline(c, pid) {
  const p = findParty(pid); if (!p || !c.uid || !p.inv.delete(c.uid)) return;
  for (const m of p.members.values()) m.send({ t: 'p_decl', name: c.acct ? c.acct.name : c.name });
  if (p.members.size <= 1 && !p.inv.size) dissolve(p); else partyBcast(p);
}
function pStart(c) {
  const p = c.party; if (!p || p.leader !== c.id) return c.err('Seul le chef de groupe peut lancer le match');
  if (p.state !== 'lobby') return;
  if (p.members.size < 2) return c.err('Un match privé se joue à plusieurs : invite un ami dans ton groupe');
  if (p.members.size > p.cfg.size * 2) return c.err('Trop de joueurs pour ce format');
  for (const x of p.members.values()) { if (!x.alive) return c.err(x.name + ' est en train de se reconnecter'); if (busy(x)) return c.err(x.name + ' est encore en match'); }
  unqueue(c); rebalanceParty(p);
  const room = { players: new Map(), host: p.leader, priv: true, state: 'play', cfg: Object.assign({}, p.cfg), code: null, party: p, map: MAP_LIST.indexOf(p.cfg.map) >= 0 ? p.cfg.map : randomMap() };   // match privé : le chef peut choisir la map, sinon au hasard
  rooms.add(room);
  for (const m of p.members.values()) { if (m.room) leaveRoom(m); m.team = m.ptm; m.room = room; room.players.set(m.id, m); }
  p.state = 'match'; launch(room); partyBcast(p);
}
function tickSocial() {
  const now = Date.now();
  for (const p of [...parties]) {
    let ch = false; for (const [u, t] of p.inv) if (t < now) { p.inv.delete(u); ch = true; }
    if (p.members.size <= 1 && !p.inv.size) dissolve(p); else if (ch) partyBcast(p);
  }
  for (const c of online.values()) { if (!c.acct) continue; const st = stOf(c); if (st !== c.lastSt) { c.lastSt = st; pushSt(c, st); } }
}

/* ---------- messages ---------- */
const RELAY_ALL = { in: 1 }, RELAY_HOST = { snap: 1, goal: 1, kick: 1 };
function handle(c, m, str) {
  const t = m.t;
  if (RELAY_ALL[t] || RELAY_HOST[t]) {
    const r = c.room; if (!r || r.state !== 'play') return;
    if (RELAY_HOST[t] && c.id !== r.host) return;
    const b = Buffer.from(str), lossy = t === 'in' || t === 'snap';     // encodé une seule fois pour tous les joueurs
    for (const o of r.players.values()) {
      if (o === c || !o.alive) continue;
      if (lossy && o.s.writableLength > 131072) continue;    // client lent : on saute les messages périmés plutôt que de le déconnecter
      o.write(1, b);
    }
    return;
  }
  switch (t) {
    case 'hello': {
      profile(c, m);
      const rs = m.resume && typeof m.resume === 'object' ? m.resume : null;
      let g = rs && ghosts.get(rs.id);
      if (!g && rs && typeof rs.tok === 'string') {           // le serveur n'a pas encore vu l'ancien socket mourir (connexion « à moitié ouverte ») : le jeton secret prouve que c'est le même joueur
        for (const o of conns) if (o !== c && o.id === rs.id && o.tok === rs.tok && o.alive) { g = o; break; }
        if (g) { g.alive = false; g.bye = true; try { g.s.destroy(); } catch (e) {} }
      }
      if (g && g.tok === rs.tok && (g.room || g.party)) {     // le joueur revient après une coupure : il retrouve sa place
        adopt(g, c);
        c.send({ t: 'welcome', id: c.id, on: conns.size, tok: c.tok, resumed: 1 });
        const r = c.room;
        if (r && r.state === 'play' && !rs.on) c.send(matchMsg(r, c));   // le match a démarré pendant la coupure
        if (c.acct) { c.send({ t: 'acct', uid: c.uid, code: fmtCode(c.acct.code), name: c.acct.name }); sendFriends(c); }
        if (c.party) partyBcast(c.party);
        break;
      }
      c.send({ t: 'welcome', id: c.id, on: conns.size, tok: c.tok });
      break;
    }
    case 'ping': c.send({ t: 'pong', ts: m.ts }); break;
    case 'auth': doAuth(c, m); break;
    case 'unauth': unbind(c); break;
    case 'fr_get': sendFriends(c); break;
    case 'fr_add': frAdd(c, m.code); break;
    case 'fr_accept': frAccept(c, String(m.uid)); break;
    case 'fr_decline': frDrop(c, String(m.uid), 'decline'); break;
    case 'fr_cancel': frDrop(c, String(m.uid), 'cancel'); break;
    case 'fr_remove': frDrop(c, String(m.uid), 'remove'); break;
    case 'p_invite': pInvite(c, String(m.uid)); break;
    case 'p_accept': pAccept(c, String(m.pid)); break;
    case 'p_decline': pDecline(c, String(m.pid)); break;
    case 'p_leave': leaveParty(c); break;
    case 'p_kick': case 'p_promote': {
      const p = c.party; if (!p || p.leader !== c.id) return c.err('Seul le chef de groupe peut faire ça');
      let tg = null; for (const x of p.members.values()) if (x.uid === String(m.uid)) tg = x;
      if (!tg || tg === c) return;
      if (t === 'p_promote') { p.leader = tg.id; unqueue(c); partyBcast(p); }
      else { leaveParty(tg); tg.err('Tu as été exclu du groupe'); }
      break;
    }
    case 'p_team': {
      const p = c.party; if (!p || p.state !== 'lobby') return;
      const tm = m.team === 1 ? 1 : 0; let n = 0; for (const o of p.members.values()) if (o.ptm === tm && o !== c) n++;
      if (n >= p.cfg.size) return c.err('Équipe complète');
      c.ptm = tm; partyBcast(p); break;
    }
    case 'p_cfg': {
      const p = c.party; if (!p || p.state !== 'lobby' || p.leader !== c.id) return;
      if (!validMode(m.mode, m.size) || isRanked(m.mode)) return c.err('Format invalide');
      if (p.members.size > m.size * 2) return c.err('Trop de joueurs pour ce format');
      p.cfg = { mode: m.mode, size: m.size, dur: [3, 5, 10].indexOf(m.dur) >= 0 ? m.dur : 3, map: MAP_LIST.indexOf(m.map) >= 0 ? m.map : 'rand' };
      rebalanceParty(p); partyBcast(p); break;
    }
    case 'p_start': pStart(c); break;
    case 'queue': {
      profile(c, m);
      if (c.room) { if (busy(c) && c.room.players.size > 1) return qerr(c, 'Déjà en match'); leaveRoom(c); }
      const p = c.party;
      if (p && p.members.size >= 2 && p.leader !== c.id) return qerr(c, 'Seul le chef de groupe peut lancer la recherche');
      if (p && p.state !== 'lobby') return qerr(c, 'Match privé en cours');
      if (!Array.isArray(m.modes)) return qerr(c, 'Playlist invalide');
      enqueue(c, m.modes);
      break;
    }
    case 'unqueue': unqueue(c); break;
    case 'create': case 'join': case 'team': case 'cfg': case 'start':
      c.err('Les salons à code n\'existent plus : invite un ami de ta liste dans ton groupe, puis lance un match privé'); break;
    case 'end': {
      const r = c.room; if (!r || r.state !== 'play' || r.host !== c.id) return;
      for (const o of r.players.values()) if (o !== c) o.raw(str);
      r.over = true; if (r.party) r.party.state = 'lobby';
      refreshParties(r);
      break;
    }
    case 'leave': unqueue(c); leaveRoom(c); if (c.party) partyBcast(c.party); break;
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
    return res.end(JSON.stringify({ online: conns.size, rooms: rooms.size, parties: parties.size, accounts: accounts.size, queues: q }));
  }
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, path.normalize(p));
  if (f !== PUB && f.indexOf(PUB + path.sep) !== 0) { res.writeHead(403); return res.end(); }   // « + path.sep » : sans lui, /../public-xyz/ passait le test
  if (p === '/index.html' && !fs.existsSync(f)) f = path.join(__dirname, 'index.html');   // mise à plat : index.html à côté de server.js
  sendFile(req, res, f);
});
/* fichiers statiques : lus une fois, gardés en mémoire (relus si modifiés) et envoyés compressés (index.html ≈ 800 Ko → ≈ 200 Ko) */
const fcache = new Map();
function sendFile(req, res, f) {
  fs.stat(f, (e, st) => {
    if (e || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Rocket Arena matchmaking server – OK (mets le jeu dans public/index.html pour le servir ici)'); }
    const ext = path.extname(f).toLowerCase(), hit = fcache.get(f);
    const go = ent => {
      const gz = ent.gz && /\bgzip\b/.test(req.headers['accept-encoding'] || '');
      const h = { 'Content-Type': TYPES[ext] || 'application/octet-stream', 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600', 'Vary': 'Accept-Encoding' };
      if (gz) h['Content-Encoding'] = 'gzip';
      res.writeHead(200, h); res.end(req.method === 'HEAD' ? undefined : (gz ? ent.gz : ent.raw));
    };
    if (hit && hit.m === st.mtimeMs) return go(hit);
    fs.readFile(f, (e2, d) => {
      if (e2) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('introuvable'); }
      const ent = { m: st.mtimeMs, raw: d, gz: null };
      if (/^\.(html|js|css|json|svg)$/.test(ext) && d.length > 1024) { try { ent.gz = zlib.gzipSync(d, { level: 6 }); } catch (e3) {} }
      if (d.length < 4 * 1024 * 1024) fcache.set(f, ent);
      go(ent);
    });
  });
}
server.on('clientError', (err, sock) => { try { if (sock.writable) sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); else sock.destroy(); } catch (e) {} });
server.on('error', e => { logErr('serveur HTTP', e); if (e && e.code === 'EADDRINUSE') process.exit(1); });
server.on('upgrade', (req, sock) => {
  sock.on('error', () => {});                                // sans écouteur, une coupure brutale (ECONNRESET) pendant la poignée de main faisait planter tout le serveur
  const key = req.headers['sec-websocket-key'], origin = req.headers.origin;
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || conns.size >= MAX_CONN ||
      (ORIGINS.length && origin && ORIGINS.indexOf(origin) < 0)) { sock.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return sock.destroy(); }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  conns.add(new Conn(sock));
});

/* chaque tâche périodique est isolée : une erreur imprévue dans l'une d'elles ne doit jamais arrêter les autres ni le serveur */
const every = (name, ms, fn) => setInterval(() => { try { fn(); } catch (e) { logErr(name, e); } }, ms);
loadAccounts();
every('tickQueues', 1000, tickQueues);
every('tickSocial', 2000, tickSocial);
every('keepalive', 10000, () => {                            // keep-alive + nettoyage des connexions mortes
  const now = Date.now();
  for (const c of [...conns]) { if (now - c.lastPong > 40000) c.drop(); else c.write(9, Buffer.alloc(0)); }
});
/* hébergeur gratuit qui s'endort (Render…) : le serveur se « visite » lui-même toutes les 10 min (RENDER_EXTERNAL_URL est fourni par Render ; KEEPALIVE_URL pour un autre hébergeur) */
const SELF_URL = process.env.KEEPALIVE_URL || process.env.RENDER_EXTERNAL_URL || '';
if (SELF_URL && require.main === module) every('auto-ping', 600000, () => { fetch(SELF_URL.replace(/\/$/, '') + '/health', { signal: AbortSignal.timeout(10000) }).catch(() => {}); });

/* dernier filet de sécurité : on journalise et on continue, au lieu de couper tous les joueurs d'un coup */
let crashes = 0;
process.on('uncaughtException', e => { logErr('uncaughtException', e); if (++crashes > 50) { saveNow(); process.exit(1); } });
process.on('unhandledRejection', e => { logErr('unhandledRejection', e); });
setInterval(() => { crashes = 0; }, 600000).unref();

if (require.main === module) server.listen(PORT, () => console.log('Matchmaking Rocket Arena sur le port ' + PORT));
const bye = () => { saveNow(); process.exit(0); };
process.on('SIGTERM', bye);
process.on('SIGINT', bye);
module.exports = { server, tickQueues, tickSocial, saveNow, _setCerts: f => { certOverride = f; }, _accounts: accounts };

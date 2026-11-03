// MAFIA — Lobby server (Express + Socket.io). Rooms live in memory.
const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  next();
});
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
// Small payload cap + fast dead-connection detection (~18s) so "disconnected" shows quickly.
const io = new Server(server, { maxHttpBufferSize: 1e4, pingInterval: 10000, pingTimeout: 8000 });

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
const CODE_LEN = 6, MIN_CAP = 8, MAX_CAP = 17, DEFAULT_CAP = 12, GRACE_MS = 45000;
const MIN_PLAYERS = MIN_CAP, MAX_PLAYERS = MAX_CAP;
const MAX_ROOMS = 2000, CREATES_PER_MIN = 6, JOIN_FAILS_PER_MIN = 10, EVENTS_PER_10S = 60;

const rooms = new Map();   // code  -> room
const tokens = new Map();  // token -> { room, pid }   (reconnect identity, survives code changes)
const retired = new Set(); // codes of closed/invalidated rooms; never reissued (bounded)

function retire(code) {
  retired.add(code);
  if (retired.size > 5000) retired.delete(retired.values().next().value);
}
function genCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < CODE_LEN; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c) && !retired.has(c)) return c;
  }
}
// Strip control / bidi / zero-width characters so names can't spoof or vanish.
const cleanName = n => String(n ?? '').replace(/[\u0000-\u001f\u007f\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g, '')
  .replace(/\s+/g, ' ').trim().slice(0, 16);
const normCode = c => String(c ?? '').toUpperCase().replace(/[^A-Z0-9]/g, '');

// ---- per-IP rate limits (in-memory) ----
const hits = { create: new Map(), fail: new Map() };
function rate(map, ip, windowMs, add) {
  const now = Date.now();
  const a = (map.get(ip) || []).filter(t => now - t < windowMs);
  if (add) a.push(now);
  map.set(ip, a);
  return a.length;
}
setInterval(() => { for (const m of Object.values(hits)) for (const k of m.keys()) if (!rate(m, k, 60000)) m.delete(k); }, 300000).unref();
const ipOf = s => (process.env.TRUST_PROXY && String(s.handshake.headers['x-forwarded-for'] || '').split(',')[0].trim()) || s.handshake.address;

// ---- Roles (server-side only; never part of view()) ----
const ROLES = {
  sheikh:    { name: 'شيخ المافيا',    team: 'mafia' },     // unique
  silencer:  { name: 'مافيا التسكيت',  team: 'mafia' },     // unique
  mafia:     { name: 'مافيا',          team: 'mafia' },     // plain mafia seat, only used when mafia count > 2 (name assumed, see REPORT)
  sniper:    { name: 'القناص الصالح',  team: 'citizens' },  // unique
  detective: { name: 'المحقق',         team: 'citizens' },  // unique
  goodboy:   { name: 'الولد الصالح',   team: 'citizens' },  // unique
  ambulance: { name: 'الإسعاف الصالح', team: 'citizens' },  // unique
  citizen:   { name: 'المواطن العادي', team: 'citizens' },  // repeatable filler
};
const mafiaCount = n => (n >= 8 && n <= 13) ? 2 : (n >= 14 && n <= 16) ? 3 : n === 17 ? 4 : null;
function buildDeck(n) {
  const m = mafiaCount(n);
  if (!Number.isInteger(n) || m === null) throw new RangeError(`عدد اللاعبين ${n} خارج النطاق ${MIN_PLAYERS}-${MAX_PLAYERS}`);
  const deck = ['sheikh', 'silencer'];                       // the sheikh counts inside the mafia total
  for (let i = 0; i < m - 2; i++) deck.push('mafia');
  deck.push('sniper', 'detective', 'goodboy', 'ambulance');  // all four special citizens from 8 players up
  while (deck.length < n) deck.push('citizen');
  return deck;
}
function shuffle(a) { // Fisher–Yates with a CSPRNG
  for (let i = a.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [a[i], a[j]] = [a[j], a[i]]; }
  return a;
}
function assignRoles(room) { // one-shot: locked forever once set
  if (room.roles) throw new Error('الأدوار وُزّعت بالفعل');
  const ids = [...room.players.keys()];
  const deck = shuffle(buildDeck(ids.length));
  room.roles = new Map(ids.map((id, i) => [id, deck[i]]));
}
// The ONLY role information that ever leaves the server: a player's own role.
const roleView = (room, pid) => { const id = room.roles?.get(pid); return id ? { id, name: ROLES[id].name, team: ROLES[id].team } : undefined; };

function view(room) {
  return {
    code: room.code, hostId: room.hostId, max: room.max, started: room.started,
    players: [...room.players.values()].map(p => ({
      id: p.id, name: p.name, ready: p.ready, mic: p.mic, connected: p.connected, isHost: p.id === room.hostId,
    })), // never includes tokens
  };
}
const push = room => io.to(room.code).emit('room:state', view(room));

function startError(room) {
  const ps = [...room.players.values()];
  if (room.started || room.roles) return 'المباراة بدأت بالفعل';
  if (ps.length < MIN_PLAYERS) return `الحد الأدنى ${MIN_PLAYERS} لاعبين`;
  if (ps.length > MAX_PLAYERS) return `الحد الأقصى ${MAX_PLAYERS} لاعباً`;
  if (ps.length < room.max) return 'لم يكتمل عدد اللاعبين';
  if (ps.some(p => !p.connected)) return 'يوجد لاعب غير متصل';
  if (ps.some(p => p.id !== room.hostId && !p.ready)) return 'ليس كل اللاعبين جاهزين';
  return null;
}

function newPlayer(room, name, ready) {
  let id; do { id = crypto.randomUUID().slice(0, 8); } while (room.players.has(id));
  const p = { id, token: crypto.randomBytes(24).toString('hex'), name, ready, mic: true, connected: true };
  room.players.set(id, p);
  tokens.set(p.token, { room, pid: id });
  return p;
}
function bind(socket, room, p) {
  clearTimeout(p.timer);
  p.socketId = socket.id; p.connected = true;
  socket.data = { code: room.code, pid: p.id };
  socket.join(room.code);
}
function newHost(room) {
  const ps = [...room.players.values()];
  const next = ps.find(p => p.connected) || ps[0];
  if (next) room.hostId = next.id;
}
function dropRoom(room) {
  room.players.forEach(p => { clearTimeout(p.timer); tokens.delete(p.token); });
  rooms.delete(room.code); retire(room.code);
}
function closeRoom(room, reason) {
  io.to(room.code).emit('room:closed', { reason });
  io.in(room.code).socketsLeave(room.code);
  dropRoom(room);
}
function removePlayer(room, pid, kickReason) {
  const p = room.players.get(pid);
  if (!p) return;
  clearTimeout(p.timer); tokens.delete(p.token);
  room.players.delete(pid); room.roles?.delete(pid);
  const s = io.sockets.sockets.get(p.socketId);
  if (s) { if (kickReason) s.emit('room:kicked', { reason: kickReason }); s.leave(room.code); s.data = {}; }
  if (room.players.size === 0) return dropRoom(room);
  if (room.hostId === pid) newHost(room);
  push(room);
}

io.on('connection', socket => {
  const ip = ipOf(socket);
  let evCount = 0, evStart = Date.now();

  // Safe handler: tolerates null/garbage payloads or a missing ack, never lets an exception crash the process.
  const on = (ev, fn) => socket.on(ev, (...args) => {
    const last = args[args.length - 1];
    const cb = typeof last === 'function' ? last : () => {};
    const data = args[0] && typeof args[0] === 'object' ? args[0] : {};
    if (Date.now() - evStart > 10000) { evStart = Date.now(); evCount = 0; }
    if (++evCount > EVENTS_PER_10S) return cb({ ok: false, error: 'طلبات كثيرة، تمهّل قليلاً' });
    try { fn(data, cb); } catch (e) { console.error(ev, e); cb({ ok: false, error: 'خطأ في الخادم' }); }
  });
  const ctx = () => {
    const room = rooms.get(socket.data?.code);
    const me = room?.players.get(socket.data?.pid);
    return room && me ? { room, me } : null;
  };
  const hostCtx = cb => {
    const c = ctx();
    if (!c) { cb({ ok: false, error: 'أنت لست في غرفة' }); return null; }
    if (c.room.hostId !== c.me.id) { cb({ ok: false, error: 'هذا الإجراء للمضيف فقط' }); return null; }
    return c;
  };
  const session = (room, p) => ({ ok: true, token: p.token, me: p.id, state: view(room), role: roleView(room, p.id) });
  const NO_SESSION = { ok: false, code: 'NO_SESSION', error: 'انتهت الجلسة' };

  function resumeWith(ent, cb) {
    const room = ent.room, p = room.players.get(ent.pid);
    if (!p || rooms.get(room.code) !== room) return cb(NO_SESSION);
    const cur = ctx();
    if (cur && cur.me.id !== p.id) return cb({ ok: false, error: 'أنت بالفعل داخل غرفة أخرى' });
    const old = io.sockets.sockets.get(p.socketId);
    if (old && old.id !== socket.id) { old.emit('session:replaced'); old.leave(room.code); old.data = {}; }
    bind(socket, room, p);
        if (
      room.match &&
      room.match.phase === 'night' &&
      room.match.alive.includes(p.id)
    ) {
      const current = nightOrder.find(
        x => x.step === room.match.nightStep
      );

      if (current && room.roles.get(p.id) === current.role) {
        sendNightTurn(room);
      }
        }
    cb(session(room, p)); push(room);
  }

  on('room:create', ({ name }, cb) => {
    if (ctx()) return cb({ ok: false, error: 'أنت بالفعل داخل غرفة' });
    name = cleanName(name);
    if (!name) return cb({ ok: false, error: 'اكتب اسمك أولاً' });
    if (rooms.size >= MAX_ROOMS) return cb({ ok: false, error: 'الخادم ممتلئ حالياً، حاول لاحقاً' });
    if (rate(hits.create, ip, 60000, true) > CREATES_PER_MIN) return cb({ ok: false, error: 'أنشأت غرفاً كثيرة، انتظر دقيقة' });
    const room = { code: genCode(), hostId: null, max: DEFAULT_CAP, started: false, roles: null, players: new Map() };
    rooms.set(room.code, room);
    const p = newPlayer(room, name, true);
    room.hostId = p.id;
    bind(socket, room, p);
    cb(session(room, p)); push(room);
  });

  on('room:join', ({ code, name, token }, cb) => {
    code = normCode(code);
    // Same person re-joining with their own token = resume, never a duplicate seat.
    const ent = typeof token === 'string' && tokens.get(token);
    if (ent && ent.room.code === code) return resumeWith(ent, cb);
    if (ctx()) return cb({ ok: false, error: 'أنت بالفعل داخل غرفة' });
    if (rate(hits.fail, ip, 60000) >= JOIN_FAILS_PER_MIN) return cb({ ok: false, error: 'محاولات كثيرة، انتظر دقيقة' });
    name = cleanName(name);
    if (!name) return cb({ ok: false, error: 'اكتب اسمك أولاً' });
    const room = rooms.get(code);
    if (!room) { rate(hits.fail, ip, 60000, true); return cb({ ok: false, error: 'الرمز غير صحيح أو الغرفة مغلقة' }); }
    if (room.started) return cb({ ok: false, error: 'بدأت المباراة في هذه الغرفة' });
    if (room.players.size >= room.max) return cb({ ok: false, error: 'الغرفة مكتملة' });
    const lower = name.toLowerCase();
    if ([...room.players.values()].some(x => x.name.toLowerCase() === lower)) return cb({ ok: false, error: 'الاسم مستخدم في الغرفة، اختر اسماً آخر' });
    const p = newPlayer(room, name, false);
    bind(socket, room, p);
    cb(session(room, p)); push(room);
  });

  // Reconnect after a network drop. Looked up by secret token only (not by code),
  // so it still works if the host changed the room code while the player was offline.
  on('room:resume', ({ token }, cb) => {
    const ent = typeof token === 'string' && tokens.get(token);
    if (!ent) return cb(NO_SESSION);
    resumeWith(ent, cb);
  });

  on('player:ready', ({ ready }, cb) => {
    const c = ctx(); if (!c) return cb({ ok: false });
    if (c.room.started) return cb({ ok: false });
    c.me.ready = !!ready; cb({ ok: true }); push(c.room);
  });
  on('player:mic', ({ mic }, cb) => {
    const c = ctx(); if (!c) return cb({ ok: false });
    c.me.mic = !!mic; cb({ ok: true }); push(c.room);
  });
  on('room:leave', (_, cb) => {
    const c = ctx(); if (!c) return cb({ ok: true });
    removePlayer(c.room, c.me.id); cb({ ok: true });
  });

  // ---- Host-only actions (verified server-side) ----
  on('host:kick', ({ id }, cb) => {
    const c = hostCtx(cb); if (!c) return;
    if (c.room.started) return cb({ ok: false, error: 'لا يمكن الطرد بعد بدء المباراة' });
    if (id === c.me.id) return cb({ ok: false, error: 'لا يمكنك طرد نفسك' });
    if (!c.room.players.has(id)) return cb({ ok: false, error: 'اللاعب غير موجود' });
    removePlayer(c.room, id, 'طردك المضيف من الغرفة'); cb({ ok: true });
  });
  on('host:transfer', ({ id }, cb) => {
    const c = hostCtx(cb); if (!c) return;
    const t = c.room.players.get(id);
    if (!t || id === c.me.id) return cb({ ok: false, error: 'لاعب غير صالح' });
    if (!t.connected) return cb({ ok: false, error: 'اللاعب غير متصل' });
    c.room.hostId = id; cb({ ok: true }); push(c.room);
  });
  on('host:settings', ({ max }, cb) => {
    const c = hostCtx(cb); if (!c) return;
    if (c.room.started) return cb({ ok: false, error: 'لا يمكن تغيير الإعدادات بعد البدء' });
    max = parseInt(max, 10);
    if (!(max >= MIN_CAP && max <= MAX_CAP)) return cb({ ok: false, error: `السعة بين ${MIN_CAP} و${MAX_CAP}` });
    if (max < c.room.players.size) return cb({ ok: false, error: 'السعة أقل من عدد اللاعبين الحاليين' });
    c.room.max = max; cb({ ok: true }); push(c.room);
  });
  on('host:changeCode', (_, cb) => {
    const c = hostCtx(cb); if (!c) return;
    const { room } = c, old = room.code, fresh = genCode();
    rooms.delete(old); retire(old);
    room.code = fresh; rooms.set(fresh, room);
    io.in(old).socketsJoin(fresh); io.in(old).socketsLeave(old);
    room.players.forEach(p => { const s = io.sockets.sockets.get(p.socketId); if (s && p.connected) s.data = { code: fresh, pid: p.id }; });
    cb({ ok: true }); push(room);
  });
  on('host:close', (_, cb) => {
    const c = hostCtx(cb); if (!c) return;
    closeRoom(c.room, 'أغلق المضيف الغرفة'); cb({ ok: true });
  });
  on('host:start', (_, cb) => {
    const c = hostCtx(cb); if (!c) return;
    const err = startError(c.room);
    if (err) return cb({ ok: false, error: err });
    assignRoles(c.room);               // random, server-side, one time only; throws (-> error ack) before anything changes
    c.room.started = true;
c.room.match = createMatchState(c.room);
    // Each player gets ONLY their own role, on their own socket. Nothing role-related goes to the room channel.
    c.room.players.forEach(p => { const s = io.sockets.sockets.get(p.socketId); if (s) s.emit('role:assigned', roleView(c.room, p.id)); });
    io.to(c.room.code).emit('match:started');
sendNightTurn(c.room);
push(c.room);
  });

  
  // ===== NIGHT ACTIONS =====
  const nightOrder = [
    { step: 'sheikh', role: 'sheikh', action: 'mafiaTarget' },
    { step: 'silencer', role: 'silencer', action: 'silencedTarget' },
    { step: 'ambulance', role: 'ambulance', action: 'ambulanceTarget' },
    { step: 'sniper', role: 'sniper', action: 'sniperTarget' },
    { step: 'detective', role: 'detective', action: 'detectiveTarget' }
  ];
  const nightOrder = [
    { step: 'sheikh', role: 'sheikh', action: 'mafiaTarget' },
    { step: 'silencer', role: 'silencer', action: 'silencedTarget' },
    { step: 'ambulance', role: 'ambulance', action: 'ambulanceTarget' },
    { step: 'sniper', role: 'sniper', action: 'sniperTarget' },
    { step: 'detective', role: 'detective', action: 'detectiveTarget' }
  ];

  function advanceNightTurn(room) {
    const match = room.match;
    if (!match || match.phase !== 'night') return;

    clearTimeout(match.turnTimer);
    match.turnTimer = null;

    const currentIndex = nightOrder.findIndex(
      x => x.step === match.nightStep
    );

    if (currentIndex === -1) return;

    const nextIndex = currentIndex + 1;

    if (nextIndex < nightOrder.length) {
      match.nightStep = nightOrder[nextIndex].step;
      sendNightTurn(room);
    } else {
      match.phase = 'day';
      match.nightStep = null;

      io.to(room.code).emit('day:started', {
        round: match.round
      });
    }
  }

  function sendNightTurn(room) {
    const match = room.match;
    if (!match || match.phase !== 'night') return;

    const current = nightOrder.find(
      x => x.step === match.nightStep
    );

    if (!current) return;

    const actor = [...room.roles.entries()].find(
      ([id, role]) =>
        role === current.role && match.alive.includes(id)
    );

    // إذا لم يعد صاحب الدور موجوداً أو خرج من المباراة
    if (!actor || !room.players.has(actor[0])) {
      advanceNightTurn(room);
      return;
    }

    const player = room.players.get(actor[0]);
    const s = player && io.sockets.sockets.get(player.socketId);

    // إذا كان اللاعب غير متصل، انتظر 45 ثانية ثم تجاوز دوره
    if (!s || !player.connected) {
      clearTimeout(match.turnTimer);

      const waitingStep = match.nightStep;

      match.turnTimer = setTimeout(() => {
        if (
          room.match !== match ||
          match.phase !== 'night' ||
          match.nightStep !== waitingStep
        ) return;

        advanceNightTurn(room);
      }, GRACE_MS);

      return;
    }

    clearTimeout(match.turnTimer);
    match.turnTimer = null;

    s.emit('night:your_turn', {
      step: current.step,
      message: 'حان دورك لاتخاذ قرار الليل'
    });
      }

  on('night:action', ({ targetId }, cb) => {
    const c = ctx();
    if (!c) return cb({ ok: false, error: 'أنت لست في الغرفة' });

    const room = c.room;
    const match = room.match;

    if (!match || match.phase !== 'night')
      return cb({ ok: false, error: 'ليس وقت الليل' });

    const current = nightOrder.find(x => x.step === match.nightStep);
    if (!current)
      return cb({ ok: false, error: 'مرحلة غير صالحة' });

    if (room.roles.get(c.me.id) !== current.role)
      return cb({ ok: false, error: 'ليس دورك الآن' });

    if (!match.alive.includes(c.me.id))
      return cb({ ok: false, error: 'أنت خارج المباراة' });

    if (typeof targetId !== 'string' ||
        !match.alive.includes(targetId) ||
        targetId === c.me.id)
      return cb({ ok: false, error: 'الهدف غير صالح' });

    match.actions[current.action] = targetId;

    if (current.step === 'detective') {
      const targetRole = room.roles.get(targetId);
      const targetTeam = ROLES[targetRole].team;

      const s = io.sockets.sockets.get(c.me.socketId);
      if (s) s.emit('detective:result', {
        targetId,
        team: targetTeam
      });
    }

    const nextIndex = nightOrder.findIndex(
      x => x.step === match.nightStep
    ) + 1;

    if (nextIndex < nightOrder.length) {
      match.nightStep = nightOrder[nextIndex].step;
      sendNightTurn(room);
    } else {
      match.phase = 'day';
      match.nightStep = null;
      io.to(room.code).emit('day:started', {
        round: match.round
      });
    }

    cb({ ok: true });
  });
  
  socket.on('disconnect', () => {
    const c = ctx(); if (!c || c.me.socketId !== socket.id) return;
    const { room, me } = c;
    me.connected = false; push(room);
    clearTimeout(me.timer);
    me.timer = setTimeout(() => {
      if (rooms.get(room.code) !== room || me.connected || !room.players.has(me.id)) return;
      removePlayer(room, me.id);
    }, GRACE_MS);
  });
});

module.exports = { ROLES, buildDeck, mafiaCount, assignRoles, shuffle, MIN_PLAYERS, MAX_PLAYERS };

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`MAFIA server on :${PORT}`));
// ========================================
// MAFIA MATCH ENGINE - المرحلة الثانية
// أساس نظام المباراة
// ========================================

function createMatchState(room) {
  if (!room.started || !room.roles) {
    throw new Error('لا يمكن إنشاء مباراة قبل توزيع الأدوار');
  }

  return {
    phase: 'night',
    nightStep: 'sheikh',
    round: 1,
    alive: [...room.players.keys()],
    eliminated: [],
        actions: {
      mafiaTarget: null,
      silencedTarget: null,
      ambulanceTarget: null,
      sniperTarget: null,
      detectiveTarget: null
    },
    votes: {},
    defensePlayer: null,
    finalVotes: {},
    speechIndex: 0,
    speechTimeLimit: 60,
    tieTimeLimit: 30,
    finished: false,
    winner: null
  };
}

function isAlive(room, playerId) {
  return !!room.match &&
    room.match.alive.includes(playerId);
}
  room.match.actions = {
    mafiaTarget: null,
    silencedTarget: null,
    ambulanceTarget: null,
    sniperTarget: null,
    detectiveTarget: null
  };
}

function eliminatePlayer(room, playerId) {
  if (!isAlive(room, playerId)) return false;

  room.match.alive =
    room.match.alive.filter(id => id !== playerId);

  room.match.eliminated.push({
    id: playerId,
    role: room.roles.get(playerId)
  });

  return true;
}

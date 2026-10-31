const $ = s => document.querySelector(s);
const store = { get: k => { try { return localStorage.getItem(k); } catch { return null; } },
                set: (k, v) => { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch {} } };
const socket = io({ reconnection: true });
let S = { token: store.get('token'), code: store.get('code'), me: null, room: null, role: null };

const call = (ev, data = {}) => new Promise(res =>
  socket.timeout(8000).emit(ev, data, (err, r) => res(err ? { ok: false, error: 'تعذّر الاتصال بالخادم' } : r)));
function el(tag, props = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) k.startsWith('aria-') ? n.setAttribute(k, v) : (n[k] = v);
  kids.flat().forEach(k => n.append(k));
  return n;
}
let toastT;
function toast(msg) { const t = $('#toast'); t.textContent = msg; t.hidden = false; clearTimeout(toastT); toastT = setTimeout(() => t.hidden = true, 2600); }
function show(id) {
  ['home', 'lobby', 'match', 'role'].forEach(s => $('#' + s).hidden = s !== id);
  $('#nav').hidden = id === 'match' || id === 'role';
  document.querySelectorAll('.nav button').forEach(b => b.classList.toggle('on', b.dataset.go === (id === 'lobby' ? 'rooms' : 'home')));
}
function saveSession() { store.set('token', S.token); store.set('code', S.code); }
function leaveLocal(msg) {
  S = { token: null, code: null, me: null, room: null, role: null }; saveSession(); closeSheet(); show('home');
  if (msg) toast(msg);
}
function enter(res) {
  S.token = res.token; S.me = res.me; S.room = res.state; S.code = res.state.code; S.role = res.role || S.role; saveSession();
  show(res.state.started ? 'match' : 'lobby'); render();
}

// ---------- Connection ----------
socket.on('connect', async () => {
  $('#net').hidden = true;
  if (!S.token) return;
  for (let i = 0; i < 4; i++) {
    const r = await call('room:resume', { token: S.token });
    if (r.ok) return enter(r);
    if (r.code === 'NO_SESSION') return leaveLocal('انتهت الغرفة أو الجلسة');
    await new Promise(res => setTimeout(res, 1500)); // network/server hiccup: keep the session and retry
  }
});
socket.on('disconnect', () => $('#net').hidden = false);
socket.on('room:state', st => { S.room = st; S.code = st.code; saveSession(); render(); });
socket.on('room:closed', ({ reason }) => leaveLocal(reason));
socket.on('room:kicked', ({ reason }) => leaveLocal(reason));
// Each player receives only their own role (private event, never broadcast).
function showRole() {
  if (!S.role) return toast('لم يُوزَّع الدور بعد');
  $('#roleName').textContent = S.role.name;
  $('#roleTeam').textContent = S.role.team === 'mafia' ? 'فريق المافيا' : 'فريق المواطنين';
  $('#roleCard').className = 'rolecard ' + (S.role.team === 'mafia' ? 'mafia' : '');
  show('role');
}
socket.on('role:assigned', r => { S.role = r; showRole(); });
socket.on('match:started', () => { if (!S.role) show('match'); });
$('#btnRoleOk').onclick = () => show('match');
$('#btnShowRole').onclick = showRole;
// Same account opened in another tab: this tab goes idle but must NOT erase the shared saved session.
socket.on('session:replaced', () => { S = { token: null, code: null, me: null, room: null, role: null }; closeSheet(); show('home'); toast('فُتحت الجلسة في نافذة أخرى'); });

// ---------- Render ----------
const hue = id => [...id].reduce((a, c) => a + c.charCodeAt(0) * 37, 0) % 360;
function render() {
  const r = S.room; if (!r) return;
  const iAmHost = r.hostId === S.me, me = r.players.find(p => p.id === S.me);
  $('#code').textContent = r.code;
  $('#count').textContent = `${r.players.length}/${r.max}`;
  $('#full').hidden = r.players.length < r.max;
  $('#btnMenu').hidden = !iAmHost;
  const list = $('#players'); list.replaceChildren();
  r.players.forEach(p => {
    const st = !p.connected ? el('span', { className: 'pill dc', textContent: 'غير متصل' })
      : el('span', { className: 'pill' + (p.ready ? ' ok' : ''), textContent: p.ready ? 'جاهز' : 'غير جاهز' });
    const row = el('li', { className: 'row' + (p.connected ? '' : ' off') },
      el('div', { className: 'av', textContent: [...p.name][0], style: `background:hsl(${hue(p.id)} 45% 22%)` }),
      el('div', { className: 'nm' }, p.isHost ? el('span', { className: 'crown', textContent: '♛ ' }) : '', p.name,
        p.id === S.me ? el('span', { className: 'me', textContent: ' (أنت)' }) : ''),
      st,
      el('span', { className: 'mic ' + (p.mic ? 'on' : 'off'), textContent: p.mic ? '🎙' : '🔇' }));
    if (iAmHost && p.id !== S.me) {
      row.append(el('button', { className: 'icon sm', textContent: '⋮', 'aria-label': 'خيارات اللاعب', onclick: () => playerSheet(p) }));
    } else if (p.id === S.me) {
      row.append(el('button', { className: 'icon sm', textContent: p.mic ? '🔇' : '🎙', 'aria-label': 'الميكروفون', onclick: () => call('player:mic', { mic: !p.mic }) }));
    }
    list.append(row);
  });
  const btn = $('#btnMain'), hint = $('#startHint');
  if (iAmHost) {
    const err = startError(r);
    btn.textContent = 'بدء المباراة'; btn.disabled = !!err; hint.textContent = err || 'كل شيء جاهز';
    btn.onclick = async () => { const x = await call('host:start'); if (!x.ok) toast(x.error); };
  } else {
    btn.disabled = !me; btn.textContent = me?.ready ? 'إلغاء الجاهزية' : 'أنا جاهز';
    hint.textContent = 'بانتظار المضيف ليبدأ المباراة';
    btn.onclick = async () => { const x = await call('player:ready', { ready: !me?.ready }); if (!x.ok) toast('تعذّر التحديث'); };
  }
}
function startError(r) { // UI hint only; the server decides
  if (r.players.length < r.max) return `بانتظار اللاعبين (${r.players.length}/${r.max})`;
  if (r.players.some(p => !p.connected)) return 'يوجد لاعب غير متصل';
  if (r.players.some(p => p.id !== r.hostId && !p.ready)) return 'ليس كل اللاعبين جاهزين';
  return null;
}

// ---------- Sheets ----------
function openSheet(...kids) { const p = $('#sheet .panel'); p.replaceChildren(...kids); $('#sheet').hidden = false; }
function closeSheet() { $('#sheet').hidden = true; }
$('#sheet').addEventListener('click', e => { if (e.target.id === 'sheet') closeSheet(); });
const act = (label, cls, fn) => el('button', { className: 'btn ' + cls, textContent: label, onclick: fn });

function playerSheet(p) {
  openSheet(el('h3', { textContent: p.name }),
    act('نقل صلاحيات المضيف إليه', 'ghost', async () => { const x = await call('host:transfer', { id: p.id }); closeSheet(); toast(x.ok ? 'تم نقل الصلاحيات' : x.error); }),
    act('طرد من الغرفة', 'danger', async () => { const x = await call('host:kick', { id: p.id }); closeSheet(); if (!x.ok) toast(x.error); }));
}
$('#btnMenu').onclick = () => {
  const cap = el('input', { type: 'number', min: 8, max: 17, value: S.room.max, inputMode: 'numeric' });
  const err = el('div', { className: 'err' });
  openSheet(el('h3', { textContent: 'إعدادات الغرفة' }),
    el('label', { className: 'field', textContent: 'عدد اللاعبين (8–17)' }, cap),
    act('حفظ العدد', 'blue', async () => { const x = await call('host:settings', { max: cap.value }); x.ok ? (closeSheet(), toast('تم الحفظ')) : err.textContent = x.error; }),
    act('تغيير رمز الغرفة', 'ghost', async () => { const x = await call('host:changeCode'); closeSheet(); toast(x.ok ? 'تم توليد رمز جديد وأُبطل القديم' : x.error); }),
    act('إغلاق الغرفة', 'danger', async () => { await call('host:close'); closeSheet(); }),
    err);
};

// ---------- Actions ----------
const getName = () => { const n = $('#name').value.trim(); if (!n) { toast('اكتب اسمك أولاً'); $('#name').focus(); return null; } store.set('name', n); return n; };
$('#name').value = store.get('name') || '';
$('#btnCreate').onclick = async () => {
  const name = getName(); if (!name) return;
  const r = await call('room:create', { name }); r.ok ? enter(r) : toast(r.error);
};
$('#btnJoin').onclick = () => {
  const name = getName(); if (!name) return;
  const inp = el('input', { placeholder: 'مثال: 7F3Q2A', maxLength: 6, autocapitalize: 'characters', style: 'direction:ltr;text-align:center;letter-spacing:5px;font-size:24px' });
  const err = el('div', { className: 'err' });
  const go = async () => {
    const r = await call('room:join', { code: inp.value, name, token: S.token });
    r.ok ? (closeSheet(), enter(r)) : err.textContent = r.error;
  };
  inp.onkeydown = e => e.key === 'Enter' && go();
  openSheet(el('h3', { textContent: 'أدخل رمز الغرفة' }), inp, err, act('انضمام', 'blue', go));
  inp.focus();
};
$('#btnBack').onclick = async () => {
  const r = S.room;
  if (r && r.hostId === S.me && r.players.length > 1 && !confirm('ستغادر الغرفة وتنتقل صلاحيات المضيف للاعب آخر. متابعة؟')) return;
  await call('room:leave'); leaveLocal();
};
$('#btnLeaveMatch').onclick = async () => { await call('room:leave'); leaveLocal(); };
$('#btnCopy').onclick = async () => {
  try { await navigator.clipboard.writeText(S.room.code); toast('تم نسخ الرمز'); }
  catch { toast('انسخ الرمز يدوياً: ' + S.room.code); }
};
document.querySelectorAll('.nav button').forEach(b => b.onclick = () => {
  const g = b.dataset.go;
  if (g === 'home') return S.room ? show('lobby') : show('home');
  if (g === 'rooms') return S.room ? show('lobby') : toast('أنشئ غرفة أو انضم برمز');
  if (g === 'profile') { show('home'); $('#name').focus(); return; }
  toast('قريباً');
});

/* Culé Live push server: a free Cloudflare Worker that sends notifications.

   Every minute it checks Barça's matches through your data server (cule-live) and sends
   an alert to every phone that turned notifications on in the app:
   lineups announced, kick-off, goals, half-time and full time.
   Phones are saved in a KV store; nothing else about the person is kept.

   Setup in the Cloudflare dashboard (free plan is enough):
   1. Workers & Pages → Create → Worker. Name it  cule-push  → Deploy.
      Then Edit code, replace everything with this file, and Deploy.
   2. Storage & Databases → KV → Create a namespace called  cule-push.
   3. Open the cule-push worker → Settings → Bindings → Add:
        • KV namespace:    variable name  KV    → namespace cule-push
        • Service binding: variable name  DATA  → service cule-live (your data server)
   4. Settings → Trigger events → Add → Cron trigger → every minute ( * * * * * ).
   5. Visit https://cule-push.<your-subdomain>.workers.dev/status once. It should say "ok": true.

   Free plan limits this is built around: 50 outside requests and about 10 ms of work per run,
   so each minute it sends up to MAX_SENDS alerts (default 20) and the rest the next minute.
   Goals go first. On the paid plan you can add a variable MAX_SENDS = 200 in Settings → Variables.
   Highlightly allows 100 requests a day, so it only asks Highlightly for lineups (every
   5 minutes before kick-off) and for the scorer's name when a goal goes in. */

const DATA_SERVER = 'https://cule-live.yellowlion2023.workers.dev'; // used if the DATA binding is missing
const APP_URL = 'https://mamenka6.github.io/Cule-Live/';
const FD_BARCA = 81, HL_BARCA = 450963; // Barça's team number in football-data.org and Highlightly
const KINDS = { lineups: 'l', kickoff: 'k', goals: 'g', halftime: 'h', fulltime: 'f' };
const LANGS = ['en', 'es', 'fr', 'ar'];
const PUSH_HOSTS = /(^|\.)(googleapis\.com|push\.apple\.com|mozilla\.com|mozaws\.net|notify\.windows\.com)$/;
const MIN = 60e3, HOUR = 3600e3;
const season = d => d.getUTCMonth() >= 6 ? d.getUTCFullYear() : d.getUTCFullYear() - 1; // a season starts in July

export default {
  async fetch(req, env) {
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    const path = new URL(req.url).pathname.replace(/\/+$/, '') || '/';
    try {
      if (!env.KV) throw httpErr(500, 'Add the KV binding (variable name KV) in Settings → Bindings.');
      if (path === '/' || path === '/status') return json(await status(env));
      if (path === '/vapid') return json({ key: (await vapid(env)).pub });
      if (req.method !== 'POST') throw httpErr(405, 'Use POST');
      const b = await readBody(req);
      if (path === '/subscribe') return json(await subscribe(env, b));
      if (path === '/unsubscribe') return json(await unsubscribe(env, b));
      if (path === '/test') return json(await sendTest(env, b));
      throw httpErr(404, 'Not found');
    } catch (e) {
      return json({ ok: false, error: String(e.message || e) }, e.status || 500);
    }
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(tick(env).catch(e => console.log('Match check failed:', e.stack || e)));
  }
};

/* ---------- Watching the matches (runs every minute) ---------- */
async function tick(env) {
  const now = Date.now();
  const st = (await env.KV.get('state', 'json')) || {};
  const before = JSON.stringify(st);
  st.track ||= {}; st.done ||= {}; st.outbox ||= [];
  const io = { calls: 0 };
  const near = m => now >= m.ts - 75 * MIN && now <= m.ts + 4 * HOUR && !st.done[m.id];
  let active = (st.sched || []).filter(near);
  const stale = !st.schedAt || now - st.schedAt > 6 * HOUR;
  const fd = {};
  // The fixture list is checked every 6 hours, and every minute from 10 minutes before kick-off
  if (stale || active.some(m => now >= m.ts - 10 * MIN)) {
    try {
      const data = await dataGet(env, `fd/teams/${FD_BARCA}/matches`, { season: season(new Date(now)) }, io);
      const list = data.matches || [];
      list.forEach(x => { fd[x.id] = x; });
      st.sched = list.map(small).filter(m => m.ts > now - 6 * HOUR && m.ts < now + 30 * 24 * HOUR).sort((a, b) => a.ts - b.ts).slice(0, 12);
      st.schedAt = now;
      active = st.sched.filter(near);
      if (st.err) st.err = null;
    } catch (e) {
      const msg = String(e.message || e);
      if (st.err?.msg !== msg) st.err = { msg, at: new Date(now).toISOString() };
    }
  }
  for (const m of active) {
    try { await follow(env, st, m, fd[m.id], now, io); }
    catch (e) { console.log('Match', m.id, e.stack || e); }
  }
  // tidy up: forget matches that left the list, and alerts that are too old to be useful
  const ids = new Set((st.sched || []).map(m => String(m.id)));
  for (const k of Object.keys(st.track)) if (!ids.has(k)) delete st.track[k];
  for (const k of Object.keys(st.done)) if (!ids.has(k)) delete st.done[k];
  st.outbox = st.outbox.filter(j => now - j.at < (j.f === 'f' ? 2 * HOUR : 20 * MIN));
  const sends = st.outbox.length ? await plan(env, st, io) : [];
  // Progress is saved before sending, so if a run is ever cut short nobody gets the same alert twice
  if (JSON.stringify(st) !== before) await env.KV.put('state', JSON.stringify(st)); // only write when something changed
  if (sends.length) {
    const ctx = { v: await vapid(env), cache: new Map() };
    await Promise.all(sends.map(x => send(env, x.s, x.body, x.ttl, ctx).catch(e => console.log('Send failed:', e.message))));
  }
}

const team = t => ({ id: t?.id ?? null, n: t?.id === FD_BARCA ? 'Barça' : (t?.shortName || t?.name || '?') });
const small = x => ({ id: x.id, ts: Date.parse(x.utcDate), h: team(x.homeTeam), a: team(x.awayTeam) });

function fdState(s) {
  if (s === 'SCHEDULED' || s === 'TIMED') return 'NS';
  if (s === 'IN_PLAY' || s === 'LIVE' || s === 'EXTRA_TIME') return 'LIVE';
  if (s === 'PENALTY_SHOOTOUT') return 'PEN';
  if (s === 'PAUSED') return 'HT';
  if (s === 'FINISHED' || s === 'AWARDED') return 'FT';
  if (s === 'SUSPENDED') return 'WAIT';
  return 'OFF'; // postponed or cancelled
}
// Goals so far (a penalty shoot-out isn't counted as goals)
function fdScore(x) {
  const s = x.score || {}, ft = s.fullTime || {};
  if (s.penalties?.home != null && s.regularTime?.home != null) {
    return { h: (s.regularTime.home ?? 0) + (s.extraTime?.home ?? 0), a: (s.regularTime.away ?? 0) + (s.extraTime?.away ?? 0) };
  }
  return { h: ft.home ?? 0, a: ft.away ?? 0 };
}

async function follow(env, st, m, x, now, io) {
  const tr = st.track[m.id] ||= { s: 'NS', sc: null, sent: {} };
  const home = m.h.id === FD_BARCA;

  // Lineups: Highlightly is checked every 5 minutes from 75 minutes before kick-off
  if (!tr.sent.l && now <= m.ts + 5 * MIN && (!tr.luAt || now - tr.luAt >= 5 * MIN)) {
    tr.luAt = now;
    const hl = await hlMatchId(env, tr, m, home, io);
    if (hl) {
      const lu = unwrap(await dataGet(env, `hl/lineups/${hl}`, {}, io).catch(() => null));
      const xi = lineupText(lu, home);
      if (xi) { tr.sent.l = 1; queue(st, 'l', { t: 'lineups', m, xi }, now); }
    }
  }
  if (!x) return; // before kick-off the score isn't checked
  const s = fdState(x.status);
  if (s === 'OFF') { st.done[m.id] = 1; return; }
  if (s === 'NS' || s === 'WAIT') { if (now > m.ts + 3 * HOUR) st.done[m.id] = 1; return; }

  const sc = fdScore(x), first = tr.sc == null;
  if (!tr.sent.k) { // kick-off (skipped if the server only noticed the match late)
    tr.sent.k = 1;
    if (s === 'LIVE' && now - m.ts < 25 * MIN) queue(st, 'k', { t: 'kickoff', m }, now);
  }
  if (s !== 'PEN') {
    const prev = tr.sc || (first && now - m.ts < 25 * MIN ? { h: 0, a: 0 } : sc);
    const dh = sc.h - prev.h, da = sc.a - prev.a;
    if (dh > 0 || da > 0) {
      const one = dh + da === 1;
      const who = one ? await lastScorer(env, tr, sc.h + sc.a, io) : null;
      const min = who?.min || (x.minute ? String(x.minute) : '');
      queue(st, 'g', { t: 'goal', m, sc, side: one ? (dh ? 'h' : 'a') : null, who, min }, now);
    } else if (dh < 0 || da < 0) {
      queue(st, 'g', { t: 'nogoal', m, sc }, now);
    }
    tr.sc = sc;
  }
  if (s === 'HT' && !tr.sent.h) { tr.sent.h = 1; if (tr.s === 'LIVE') queue(st, 'h', { t: 'ht', m, sc }, now); }
  if (s === 'FT') {
    st.done[m.id] = 1;
    if (!tr.sent.f) {
      tr.sent.f = 1;
      const p = x.score?.penalties;
      if (tr.s !== 'NS') queue(st, 'f', { t: 'ft', m, sc, pen: p?.home != null ? { h: p.home, a: p.away } : null }, now);
    }
  }
  tr.s = s;
}

// Highlightly's number for this match (looked up once, at most 3 tries)
async function hlMatchId(env, tr, m, home, io) {
  if (tr.hl || (tr.hlTries || 0) >= 3) return tr.hl || null;
  tr.hlTries = (tr.hlTries || 0) + 1;
  const params = home ? { homeTeamId: HL_BARCA, season: season(new Date(m.ts)) } : { awayTeamId: HL_BARCA, season: season(new Date(m.ts)) };
  const list = unwrap(await dataGet(env, 'hl/matches', params, io).catch(() => null));
  let best = null;
  (Array.isArray(list) ? list : []).forEach(y => {
    const d = Math.abs(Date.parse(y.date) - m.ts);
    if (d < 36 * HOUR && (!best || d < best.d)) best = { id: y.id, d };
  });
  if (best) tr.hl = best.id;
  return tr.hl || null;
}

const short = n => { const w = String(n || '?').trim().split(/\s+/); return w.length > 1 ? w.slice(1).join(' ') : w[0]; };
function lineupText(lu, home) {
  const ok = T => Array.isArray(T?.initialLineup) && T.initialLineup.flat().length >= 11;
  if (!ok(lu?.homeTeam) || !ok(lu?.awayTeam)) return null; // not announced yet
  const T = [lu.homeTeam, lu.awayTeam].find(t => t.id === HL_BARCA) || (home ? lu.homeTeam : lu.awayTeam);
  const rows = T.initialLineup.map(row => (row || []).map(p => short(p.name)).join(', ')).filter(Boolean).join('; ');
  return `Barça${T.formation ? ` (${T.formation})` : ''}: ${rows}`;
}

// Who scored the latest goal, if Highlightly already has it
async function lastScorer(env, tr, total, io) {
  if (!tr.hl) return null;
  const d = unwrap(await dataGet(env, `hl/matches/${tr.hl}`, {}, io).catch(() => null));
  const det = Array.isArray(d) ? d[0] : d;
  const goals = (det?.events || []).filter(e => /^(goal|penalty|own goal)$/i.test(String(e.type || '').trim()));
  if (goals.length !== total) return null;
  const e = goals[goals.length - 1];
  return { n: e.player || '', min: String(e.time || ''), og: /own/i.test(e.type), pen: /^penalty$/i.test(String(e.type).trim()) };
}

/* ---------- Sending ---------- */
function queue(st, f, ev, now) {
  const extra = ev.sc ? `-${ev.sc.h}-${ev.sc.a}` : '';
  st.outbox.push({ f, ev, at: now, tag: `m${ev.m.id}-${ev.t}${extra}`, after: '' });
}

// Picks the alerts to send in this run (at most MAX_SENDS a minute); the rest go out next minute
async function plan(env, st, io) {
  let budget = Math.max(0, (+env.MAX_SENDS || 20) - io.calls);
  const out = [];
  if (!budget) return out;
  const subs = await listSubs(env);
  const first = { g: 0, k: 1, h: 2, f: 3, l: 4 }; // goals jump the queue, lineups wait
  for (const job of [...st.outbox].sort((a, b) => first[a.f] - first[b.f] || a.at - b.at)) {
    const todo = subs.filter(s => s.key > job.after && s.f.includes(job.f));
    const batch = todo.slice(0, budget);
    budget -= batch.length;
    const ttl = job.f === 'f' ? 6 * 3600 : 900;
    batch.forEach(s => out.push({ s, body: payload(job, s.l), ttl }));
    if (batch.length) job.after = batch[batch.length - 1].key;
    if (batch.length === todo.length) job.done = true;
    if (!budget) break;
  }
  st.outbox = st.outbox.filter(j => !j.done);
  return out;
}

const payload = (job, l) => JSON.stringify({ ...compose(job.ev, l), tag: job.tag, url: `./#match-${job.ev.m.id}`, lang: l, ts: job.at });

async function send(env, s, body, ttl, ctx) {
  const ep = new URL(s.e);
  const res = await fetch(s.e, {
    method: 'POST',
    headers: { Authorization: await vapidAuth(ctx, ep.origin), 'Content-Encoding': 'aes128gcm', 'Content-Type': 'application/octet-stream', TTL: String(ttl), Urgency: 'high' },
    body: await encrypt(s, body)
  });
  if ((res.status === 404 || res.status === 410) && s.key) { await env.KV.delete(s.key); return 'gone'; } // the phone turned them off
  return res.ok ? 'ok' : `error ${res.status}`;
}

/* ---------- Notification text ---------- */
const L = {
  en: { lineups: 'Lineups are out', kickoff: 'Kick-off!', started: (h, a) => `${h} vs ${a} has started`, goal: 'Gooool! ⚽', goalFor: t => `Goal for ${t}`, goals: 'Goals!',
    nogoal: 'Goal ruled out', ht: 'Half-time', ftW: 'Full time: Barça win! 🔵🔴', ftD: "Full time: it's a draw", ftL: 'Full time: Barça lose', og: 'own goal', pen: 'pen',
    pens: (h, a) => `${h}–${a} on penalties`, test: 'Notifications are working ✅', testBody: "You'll get Barça alerts here." },
  es: { lineups: '¡Ya hay alineaciones!', kickoff: '¡Arranca el partido!', started: (h, a) => `${h} - ${a} ya ha empezado`, goal: '¡Goooool! ⚽', goalFor: t => `Gol: ${t}`, goals: '¡Goles!',
    nogoal: 'Gol anulado', ht: 'Descanso', ftW: 'Final: ¡gana el Barça! 🔵🔴', ftD: 'Final: empate', ftL: 'Final: pierde el Barça', og: 'en propia puerta', pen: 'penalti',
    pens: (h, a) => `${h}–${a} en los penaltis`, test: 'Las notificaciones funcionan ✅', testBody: 'Aquí te llegarán los avisos del Barça.' },
  fr: { lineups: 'Les compos sont tombées', kickoff: "Coup d'envoi !", started: (h, a) => `${h} - ${a} a commencé`, goal: 'Buuuut ! ⚽', goalFor: t => `But pour ${t}`, goals: 'Buts !',
    nogoal: 'But refusé', ht: 'Mi-temps', ftW: 'Terminé : victoire du Barça ! 🔵🔴', ftD: 'Terminé : match nul', ftL: 'Terminé : défaite du Barça', og: 'csc', pen: 'pén.',
    pens: (h, a) => `${h}–${a} aux tirs au but`, test: 'Les notifications marchent ✅', testBody: 'Tu recevras ici les alertes du Barça.' },
  ar: { lineups: 'التشكيلات الرسمية', kickoff: 'انطلقت المباراة!', started: (h, a) => `${h} ضد ${a}`, goal: 'جوووول! ⚽', goalFor: t => `هدف لفريق ${t}`, goals: 'أهداف!',
    nogoal: 'إلغاء هدف', ht: 'الاستراحة', ftW: 'نهاية المباراة: فوز برشلونة! 🔵🔴', ftD: 'نهاية المباراة: تعادل', ftL: 'نهاية المباراة: خسارة برشلونة', og: 'هدف عكسي', pen: 'ركلة جزاء',
    pens: (h, a) => `${h}–${a} بركلات الترجيح`, test: 'الإشعارات تعمل ✅', testBody: 'ستصلك تنبيهات برشلونة هنا.' }
};
function compose(ev, l) {
  const t = L[l] || L.en, m = ev.m;
  const iso = s => l === 'ar' ? `⁦${s}⁩` : s; // keeps names and scores left-to-right inside Arabic text
  const line = sc => iso(`${m.h.n} ${sc.h}–${sc.a} ${m.a.n}`);
  const home = m.h.id === FD_BARCA, opp = home ? m.a.n : m.h.n;
  const us = sc => home ? sc.h : sc.a, them = sc => home ? sc.a : sc.h;
  switch (ev.t) {
    case 'lineups': return { title: t.lineups, body: iso(ev.xi) };
    case 'kickoff': return { title: t.kickoff, body: t.started(iso(m.h.n), iso(m.a.n)) };
    case 'goal': {
      const title = !ev.side ? t.goals : (ev.side === 'h') === home ? t.goal : t.goalFor(iso(opp));
      const who = ev.who?.n ? ` · ${iso(ev.who.n)}${ev.min ? ` ${ev.min}'` : ''}${ev.who.og ? ` (${t.og})` : ev.who.pen ? ` (${t.pen})` : ''}` : ev.min ? ` · ${ev.min}'` : '';
      return { title, body: line(ev.sc) + who };
    }
    case 'nogoal': return { title: t.nogoal, body: line(ev.sc) };
    case 'ht': return { title: t.ht, body: line(ev.sc) };
    case 'ft': {
      let d = us(ev.sc) - them(ev.sc);
      if (!d && ev.pen) d = (home ? ev.pen.h - ev.pen.a : ev.pen.a - ev.pen.h);
      return { title: d > 0 ? t.ftW : d < 0 ? t.ftL : t.ftD, body: line(ev.sc) + (ev.pen ? ` · ${iso(t.pens(ev.pen.h, ev.pen.a))}` : '') };
    }
  }
  return { title: 'Culé Live', body: '' };
}

/* ---------- Phones that want alerts ---------- */
function cleanSub(b) {
  const s = b?.sub || {};
  const e = String(s.endpoint || ''), p = String(s.keys?.p256dh || ''), a = String(s.keys?.auth || '');
  let u; try { u = new URL(e); } catch (_) { throw httpErr(400, 'Bad subscription'); }
  if (u.protocol !== 'https:' || !PUSH_HOSTS.test(u.hostname)) throw httpErr(400, 'Unknown push service');
  if (unb64u(p).length !== 65 || unb64u(a).length !== 16) throw httpErr(400, 'Bad subscription keys');
  const prefs = b.prefs || {};
  const f = Object.entries(KINDS).filter(([k]) => prefs[k] !== false).map(([, c]) => c).join('');
  return { e, p, a, f, l: LANGS.includes(b.lang) ? b.lang : 'en' };
}
async function subKey(endpoint) {
  const h = new Uint8Array(await crypto.subtle.digest('SHA-256', te.encode(endpoint)));
  return 'sub:' + [...h.slice(0, 16)].map(x => x.toString(16).padStart(2, '0')).join('');
}
async function subscribe(env, b) {
  const s = cleanSub(b), key = await subKey(s.e);
  const old = await env.KV.get(key, 'json');
  if (!old || ['e', 'p', 'a', 'f', 'l'].some(k => old[k] !== s[k])) { // only write when something changed
    const v = JSON.stringify(s);
    await env.KV.put(key, v, v.length < 1000 ? { metadata: s } : {});
  }
  return { ok: true, alerts: s.f };
}
async function unsubscribe(env, b) {
  const e = String(b?.endpoint || b?.sub?.endpoint || '');
  if (!e) throw httpErr(400, 'Missing endpoint');
  await env.KV.delete(await subKey(e));
  return { ok: true };
}
async function sendTest(env, b) {
  const s = cleanSub(b);
  const t = L[s.l];
  const body = JSON.stringify({ title: t.test, body: t.testBody, tag: 'test', url: './#options', lang: s.l, ts: Date.now() });
  const r = await send(env, s, body, 300, { v: await vapid(env), cache: new Map() });
  return { ok: r === 'ok', result: r };
}
async function listSubs(env) {
  const out = [];
  let cursor;
  do {
    const r = await env.KV.list({ prefix: 'sub:', cursor });
    for (const k of r.keys) {
      const s = k.metadata || await env.KV.get(k.name, 'json');
      if (s?.e) out.push({ ...s, key: k.name });
    }
    cursor = r.list_complete ? null : r.cursor;
  } while (cursor);
  return out.sort((x, y) => (x.key < y.key ? -1 : 1));
}

async function status(env) {
  await vapid(env); // makes the server's keys on the first visit
  const st = (await env.KV.get('state', 'json')) || {};
  const subs = await listSubs(env);
  const next = (st.sched || []).find(m => m.ts > Date.now() - 3 * HOUR);
  return {
    ok: true,
    phones: subs.length,
    dataServer: env.DATA ? 'service binding' : DATA_SERVER,
    lastFixtureCheck: st.schedAt ? new Date(st.schedAt).toISOString() : 'not yet: add the every-minute cron trigger, then wait a minute',
    nextMatch: next ? `${next.h.n} vs ${next.a.n}, ${new Date(next.ts).toISOString()}` : null,
    alertsWaiting: (st.outbox || []).length,
    problem: st.err || null
  };
}

/* ---------- Web Push: server keys (VAPID, RFC 8292) and encryption (RFC 8291) ---------- */
async function vapid(env) {
  const saved = await env.KV.get('vapid', 'json');
  if (saved?.pub && saved?.jwk) return saved;
  const kp = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
  const v = { pub: b64u(new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))), jwk: await crypto.subtle.exportKey('jwk', kp.privateKey) };
  await env.KV.put('vapid', JSON.stringify(v));
  return v;
}
async function vapidAuth(ctx, aud) {
  if (ctx.cache.has(aud)) return ctx.cache.get(aud);
  ctx.key ||= await crypto.subtle.importKey('jwk', ctx.v.jwk, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign']);
  const enc = o => b64u(te.encode(JSON.stringify(o)));
  const unsigned = `${enc({ typ: 'JWT', alg: 'ES256' })}.${enc({ aud, exp: Math.floor(Date.now() / 1000) + 50 * 60, sub: APP_URL })}`;
  const sig = new Uint8Array(await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, ctx.key, te.encode(unsigned)));
  const h = `vapid t=${unsigned}.${b64u(sig)}, k=${ctx.v.pub}`;
  ctx.cache.set(aud, h);
  return h;
}
async function hkdf(salt, ikm, info, bytes) {
  const k = await crypto.subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
  return new Uint8Array(await crypto.subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt, info }, k, bytes * 8));
}
async function encrypt(s, text) {
  const uaPub = unb64u(s.p), auth = unb64u(s.a);
  const uaKey = await crypto.subtle.importKey('raw', uaPub, { name: 'ECDH', namedCurve: 'P-256' }, false, []);
  const mine = await crypto.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']);
  const myPub = new Uint8Array(await crypto.subtle.exportKey('raw', mine.publicKey));
  const secret = new Uint8Array(await crypto.subtle.deriveBits({ name: 'ECDH', public: uaKey }, mine.privateKey, 256));
  const ikm = await hkdf(auth, secret, cat(te.encode('WebPush: info\0'), uaPub, myPub), 32);
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const cek = await hkdf(salt, ikm, te.encode('Content-Encoding: aes128gcm\0'), 16);
  const nonce = await hkdf(salt, ikm, te.encode('Content-Encoding: nonce\0'), 12);
  const key = await crypto.subtle.importKey('raw', cek, 'AES-GCM', false, ['encrypt']);
  const sealed = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv: nonce }, key, cat(te.encode(text), new Uint8Array([2]))));
  const head = new Uint8Array(21);
  head.set(salt);
  new DataView(head.buffer).setUint32(16, 4096);
  head[20] = myPub.length;
  return cat(head, myPub, sealed);
}

/* ---------- Small helpers ---------- */
const te = new TextEncoder();
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '86400' };
const json = (o, status = 200) => new Response(JSON.stringify(o, null, 2), { status, headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...CORS } });
const httpErr = (status, msg) => Object.assign(new Error(msg), { status });
async function readBody(req) {
  const t = await req.text();
  if (t.length > 8000) throw httpErr(413, 'Too big');
  try { return JSON.parse(t || '{}'); } catch (e) { throw httpErr(400, 'Send JSON'); }
}
async function dataGet(env, path, params, io) {
  const qs = new URLSearchParams(params).toString();
  const url = `${DATA_SERVER}/${path}${qs ? '?' + qs : ''}`;
  io.calls++;
  const init = { headers: { Accept: 'application/json', Origin: new URL(APP_URL).origin } };
  const res = env.DATA ? await env.DATA.fetch(url, init) : await fetch(url, init);
  if (!res.ok) throw new Error(`Data server answered ${res.status} for ${path}`);
  return res.json();
}
const unwrap = r => (r && !Array.isArray(r) && r.data !== undefined && !r.homeTeam ? r.data : r);
const b64u = u8 => btoa(String.fromCharCode(...u8)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
function unb64u(s) {
  try { s = String(s); const b = atob(s.replace(/-/g, '+').replace(/_/g, '/') + '==='.slice((s.length + 3) % 4)); return Uint8Array.from(b, c => c.charCodeAt(0)); }
  catch (e) { return new Uint8Array(0); }
}
const cat = (...parts) => { const o = new Uint8Array(parts.reduce((n, x) => n + x.length, 0)); let i = 0; for (const x of parts) { o.set(x, i); i += x.length; } return o; };

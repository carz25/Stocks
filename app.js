/* StockPulse: a static, browser-only watchlist on the Finnhub API.
   The API key lives in this browser's localStorage and is sent only to finnhub.io. */
(function () {
'use strict';

const API = 'https://finnhub.io/api/v1';
const WS_URL = 'wss://ws.finnhub.io';
const DEFAULT_SYMBOLS = ['AAPL','MSFT','GOOGL','NVDA','AMZN','TSLA','META','SNDK','SPCX','NFLX','ORCL','QQQM','DOT.TO','BANK.TO','RY.TO','CM.TO','TD.TO','BMO.TO'];
const CACHE_TTL = 12 * 3600 * 1000;   // profile, metrics, analyst ratings
const NEWS_TTL = 15 * 60 * 1000;
const CALL_GAP = 1100;                // ms between REST calls (free plan allows 60 a minute)

/* ---------- storage ---------- */
const LS = {
  get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch (e) { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} },
  del(k) { try { localStorage.removeItem(k); } catch (e) {} }
};

/* ---------- state ---------- */
const S = {
  key: LS.get('sp.key', ''),
  symbols: LS.get('sp.symbols', DEFAULT_SYMBOLS),
  data: {},                 // per symbol: {q, profile, metric, rec, ticks, t, live, unavailable}
  ui: Object.assign({ layout: 'table', mkt: 'All', sort: 'dp', dir: -1, frame: '1D', size: 'cap', why: null, lvl: 'All' }, LS.get('sp.ui', {})),
  log: [],
  news: {},
  status: 'none', statusText: '',
  authError: false, pauseUntil: 0, lastOk: 0, wsState: 'closed'
};
let dirty = true, queue = [], ws = null, wsRetry = 0, rr = 0, cache = LS.get('sp.cache', {});

/* ---------- helpers ---------- */
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const num = (v) => (typeof v === 'number' && isFinite(v) ? v : null);
const f2 = (n) => (num(n) == null ? '—' : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
const sg = (n, dp) => (num(n) == null ? '—' : (n >= 0 ? '+' : '−') + Math.abs(n).toFixed(dp == null ? 2 : dp));
const cls = (n) => (num(n) == null ? 'fl' : n > 0.005 ? 'up' : n < -0.005 ? 'dn' : 'fl');
const hex = (n) => (num(n) == null ? '#9AA6B8' : n > 0.005 ? '#3DD6A0' : n < -0.005 ? '#FF7A85' : '#9AA6B8');
const isCA = (sym) => /\.(TO|V|NE|CN)$/.test(sym);
const short = (sym) => sym.replace(/\.TO$/, '');
const capStr = (m) => (num(m) == null ? '—' : m >= 1e6 ? (m / 1e6).toFixed(2) + 'T' : m >= 1e3 ? (m / 1e3).toFixed(1) + 'B' : Math.round(m) + 'M');
const D = (sym) => (S.data[sym] = S.data[sym] || { ticks: [] });
const cleanSymbols = (text) => [...new Set(String(text).toUpperCase().split(/[\s,;]+/).map((s) => s.replace(/[^A-Z0-9.\-]/g, '')).filter(Boolean))].slice(0, 40);
const saveUi = () => LS.set('sp.ui', S.ui);

function log(lvl, msg) {
  const t = new Date().toLocaleTimeString('en-GB');
  S.log.push({ t, lvl, msg });
  if (S.log.length > 400) S.log.shift();
  renderLog();
}

/* ---------- market clock (US and TSX share 9:30 to 16:00 Eastern) ---------- */
function market() {
  try {
    const p = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', weekday: 'long', month: 'long', day: 'numeric', hour: 'numeric', minute: 'numeric', hour12: false }).formatToParts(new Date());
    const g = (k) => p.find((x) => x.type === k).value;
    const wd = g('weekday'), m = (+g('hour') % 24) * 60 + +g('minute'), day = wd + ', ' + g('month') + ' ' + g('day');
    if (wd === 'Saturday' || wd === 'Sunday') return { k: 'closed', text: 'Markets closed · weekend', c: '#9AA6B8', day };
    if (m < 240 || m >= 1200) return { k: 'closed', text: 'Markets closed', c: '#9AA6B8', day };
    if (m < 570) return { k: 'pre', text: 'Pre-market · opens 9:30 ET', c: '#E8B64A', day };
    if (m < 960) return { k: 'open', text: 'Markets open · close 4:00 ET', c: '#3DD6A0', day };
    return { k: 'post', text: 'After hours', c: '#E8B64A', day };
  } catch (e) { return { k: 'closed', text: '', c: '#9AA6B8', day: '' }; }
}

/* ---------- Finnhub REST queue ---------- */
function api(path, pri) {
  return new Promise((res, rej) => { queue.push({ path, pri: pri || 1, res, rej }); queue.sort((a, b) => a.pri - b.pri); });
}
async function pump() {
  if (!S.key || S.authError || !queue.length || Date.now() < S.pauseUntil) return;
  const t = queue.shift();
  try {
    const r = await fetch(API + t.path + (t.path.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(S.key));
    if (r.status === 429) {
      queue.unshift(t); S.pauseUntil = Date.now() + 30000;
      log('WARN', 'Finnhub rate limit reached (HTTP 429) · pausing requests for 30s');
    } else if (r.status === 401) {
      S.authError = true; queue = []; t.rej({ code: 401 });
      log('ERROR', 'Finnhub rejected the API key (HTTP 401) · check it in Configuration');
    } else if (r.status === 403) {
      t.rej({ code: 403 });
    } else if (!r.ok) {
      t.rej({ code: r.status }); log('ERROR', 'Finnhub HTTP ' + r.status + ' on ' + t.path.split('?')[0]);
    } else {
      S.lastOk = Date.now(); t.res(await r.json());
    }
  } catch (e) {
    t.rej({ code: 0 }); log('ERROR', 'Network error calling Finnhub ' + t.path.split('?')[0]);
  }
  updateStatus();
}

function setQuote(sym, q) {
  const d = D(sym);
  if (!q || !num(q.c) || q.c === 0) { d.unavailable = 'No quote returned for this symbol'; log('WARN', sym + ': no quote returned · check the symbol'); dirty = true; return; }
  d.unavailable = null; d.q = q; d.t = (q.t || 0) * 1000; d.live = false;
  if (!d.ticks.length) { d.ticks = [q.pc, q.o, q.c].filter((v) => num(v) && v > 0); d.lastTick = Date.now(); } else pushTick(d, q.c);
  subscribe(sym);
  dirty = true;
}
function pushTick(d, p) {
  const now = Date.now();
  if (now - (d.lastTick || 0) > 5000) { d.ticks.push(p); d.lastTick = now; if (d.ticks.length > 2000) d.ticks.shift(); }
  else d.ticks[d.ticks.length - 1] = p;
}
function fetchQuote(sym, pri) {
  return api('/quote?symbol=' + encodeURIComponent(sym), pri).then((q) => setQuote(sym, q)).catch((e) => {
    if (e.code === 403) { const d = D(sym); if (!d.unavailable) log('WARN', sym + ' is not included in your Finnhub plan'); d.unavailable = 'Not included in your Finnhub plan'; dirty = true; }
  });
}
function fetchExtras(sym) {
  const d = D(sym), c = cache[sym];
  if (c && Date.now() - c.ts < CACHE_TTL) { d.profile = c.profile; d.metric = c.metric; d.rec = c.rec; return; }
  const e = encodeURIComponent(sym), out = { ts: Date.now() };
  const done = () => { cache[sym] = out; LS.set('sp.cache', cache); dirty = true; };
  api('/stock/profile2?symbol=' + e, 2).then((j) => { d.profile = out.profile = j && j.name ? j : null; done(); }).catch(() => {});
  api('/stock/metric?symbol=' + e + '&metric=all', 3).then((j) => { d.metric = out.metric = j && j.metric ? j.metric : null; done(); }).catch(() => {});
  api('/stock/recommendation?symbol=' + e, 4).then((j) => { d.rec = out.rec = Array.isArray(j) && j.length ? j[0] : null; done(); }).catch(() => {});
}
function loadAll() {
  if (!S.key) return;
  log('INFO', 'Loading ' + S.symbols.length + ' symbols: ' + S.symbols.join(' '));
  S.symbols.forEach((s) => fetchQuote(s, 0).then(() => { if (!D(s).unavailable) fetchExtras(s); }));
}
function refreshNext() {
  if (!S.key || S.authError || queue.length > 4 || !S.symbols.length) return;
  for (let i = 0; i < S.symbols.length; i++) {
    const s = S.symbols[(rr++) % S.symbols.length];
    if (!D(s).unavailable) { fetchQuote(s, 1); return; }
  }
}
function fetchNews(sym) {
  const n = S.news[sym];
  if (!S.key || (n && Date.now() - n.ts < NEWS_TTL)) return;
  S.news[sym] = { ts: Date.now(), items: n ? n.items : null };
  const iso = (dt) => dt.toISOString().slice(0, 10), to = new Date(), from = new Date(Date.now() - 7 * 864e5);
  api('/company-news?symbol=' + encodeURIComponent(sym) + '&from=' + iso(from) + '&to=' + iso(to), 1)
    .then((j) => { S.news[sym] = { ts: Date.now(), items: Array.isArray(j) ? j.slice(0, 8) : [] }; dirty = true; })
    .catch(() => { S.news[sym] = { ts: Date.now(), items: [] }; dirty = true; });
}

/* ---------- Finnhub WebSocket (live trades) ---------- */
// Subscribe only to symbols Finnhub has already returned a quote for, so unsupported ones are never sent.
function subscribe(sym) {
  const d = D(sym);
  if (!ws || S.wsState !== 'open' || d.sub || !d.q || d.unavailable) return;
  try { ws.send(JSON.stringify({ type: 'subscribe', symbol: sym })); d.sub = true; } catch (e) {}
}
function wsConnect() {
  if (!S.key || S.authError) return;
  if (ws) { try { ws.onclose = null; ws.close(); } catch (e) {} }
  S.wsState = 'connecting'; log('INFO', 'Finnhub WebSocket connecting');
  try { ws = new WebSocket(WS_URL + '?token=' + encodeURIComponent(S.key)); } catch (e) { S.wsState = 'closed'; return; }
  ws.onopen = () => {
    S.wsState = 'open'; wsRetry = 0; log('INFO', 'Finnhub WebSocket connected');
    S.symbols.forEach((s) => { D(s).sub = false; subscribe(s); });
    updateStatus();
  };
  ws.onmessage = (ev) => {
    let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
    if (m.type === 'trade' && Array.isArray(m.data)) {
      m.data.forEach((tr) => {
        const d = S.data[tr.s]; if (!d || !d.q || !num(tr.p)) return;
        const q = d.q; q.c = tr.p; if (num(q.pc) && q.pc) { q.d = tr.p - q.pc; q.dp = q.d / q.pc * 100; }
        if (!num(q.h) || tr.p > q.h) q.h = tr.p; if (!num(q.l) || q.l === 0 || tr.p < q.l) q.l = tr.p;
        d.t = tr.t || Date.now(); d.live = true; pushTick(d, tr.p);
      });
      S.lastOk = Date.now(); dirty = true;
    } else if (m.type === 'error') { log(/invalid symbol/i.test(m.msg || '') ? 'WARN' : 'ERROR', 'Finnhub WebSocket: ' + (m.msg || 'error')); }
  };
  ws.onerror = () => { log('ERROR', 'Finnhub WebSocket error'); };
  ws.onclose = () => {
    S.wsState = 'closed'; const wait = Math.min(60, 5 * Math.pow(2, wsRetry++));
    log('WARN', 'Finnhub WebSocket closed · retrying in ' + wait + 's'); updateStatus();
    setTimeout(wsConnect, wait * 1000);
  };
}

/* ---------- status dot: red = error or no key, yellow = rate limit, green = live ---------- */
function updateStatus() {
  let st, text;
  if (!S.key) { st = 'none'; text = 'Not configured · add your Finnhub API key'; }
  else if (S.authError) { st = 'err'; text = 'Finnhub rejected the API key'; }
  else if (Date.now() < S.pauseUntil) { st = 'rate'; text = 'Finnhub rate limit reached · updates paused briefly'; }
  else if (!S.lastOk) { st = 'wait'; text = 'Connecting to Finnhub'; }
  else if (Date.now() - S.lastOk > 120000) { st = 'err'; text = 'No data from Finnhub for 2 minutes · see the log'; }
  else { st = 'ok'; text = S.wsState === 'open' ? 'Live · connected and updating' : 'Updating by polling · live stream reconnecting'; }
  if (st === S.status && text === S.statusText) return;
  S.status = st; S.statusText = text;
  ['#st-dot', '#st-dot2'].forEach((id) => { const el = $(id); if (el) el.className = 'status-dot ' + st; });
  $('#st-text').textContent = text;
  $('#st-banner').style.borderColor = st === 'ok' ? '#3DD6A0' : st === 'rate' ? '#E8B64A' : st === 'wait' ? '#2E3B52' : '#FF7A85';
  $('#cfg-btn').title = text; $('#cfg-btn').setAttribute('aria-label', 'Configuration, status: ' + text);
  dirty = true;
}

/* ---------- derived values ---------- */
function score(d) {
  const q = d.q, m = d.metric;
  if (!q || !m) return null;
  const fs = [], add = (v, t) => { if (v) fs.push({ v, t }); };
  const r = d.rec;
  if (r) { const tot = r.strongBuy + r.buy + r.hold + r.sell + r.strongSell, b = r.strongBuy + r.buy; if (tot > 0) add(Math.round((b / tot - 0.5) * 40), b + ' of ' + tot + ' analysts rate it a buy'); }
  const hi = num(m['52WeekHigh']), lo = num(m['52WeekLow']);
  if (hi && lo && hi > lo) {
    const pos = (q.c - lo) / (hi - lo) * 100, off = (q.c / hi - 1) * 100;
    if (pos > 92) add(-9, 'Sitting near its 52-week high');
    else if (off <= -5 && off >= -20) add(5, Math.abs(off).toFixed(0) + '% below its 52-week high');
    else if (pos < 20) add(-4, 'Near its 52-week low, the trend is weak');
  }
  const r13 = num(m['13WeekPriceReturnDaily']); if (r13 != null) add(r13 > 0 ? 5 : -5, '13-week return is ' + sg(r13, 1) + '%');
  const r52 = num(m['52WeekPriceReturnDaily']); if (r52 != null) add(r52 > 0 ? 4 : -4, '1-year return is ' + sg(r52, 1) + '%');
  const pe = peOf(m); if (pe != null && pe > 0) { if (pe > 60) add(-8, 'High P/E of ' + pe.toFixed(0)); else if (pe < 20) add(5, 'Modest P/E of ' + pe.toFixed(0)); }
  if (num(q.dp) != null) { if (q.dp <= -3) add(3, 'Down ' + Math.abs(q.dp).toFixed(1) + '% today, a cheaper entry'); else if (q.dp >= 4) add(-3, 'Up ' + q.dp.toFixed(1) + '% today, risk of chasing a jump'); }
  if (!fs.length) return null;
  const sc = Math.round(Math.max(8, Math.min(94, 50 + fs.reduce((a, f) => a + f.v, 0))));
  return { sc, fs, k: sc >= 68 ? 'buy' : sc >= 48 ? 'wait' : 'no', label: sc >= 68 ? 'Good time to buy' : sc >= 48 ? 'Wait for a dip' : 'Not now' };
}
const peOf = (m) => (m ? (num(m.peTTM) != null ? m.peTTM : num(m.peBasicExclExtraTTM)) : null);
const ICON = { buy: 'M5 12.5l4.5 4.5L19 7.5', wait: 'M9 6v12M15 6v12', no: 'M6 6l12 12M18 6L6 18', na: 'M6 12h12' };
function pill(sc, sym, interactive) {
  const k = sc ? sc.k : 'na', label = sc ? sc.label : 'Not enough data';
  const inner = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="' + ICON[k] + '"/></svg>' + label;
  return interactive && sc
    ? '<button class="pill ' + k + '" data-act="why" data-sym="' + esc(sym) + '" aria-expanded="' + (S.ui.why === sym) + '" title="Why this score?">' + inner + '</button>'
    : '<span class="pill static ' + k + '">' + inner + '</span>';
}
function fresh(d) {
  if (!d.t) return { text: 'Waiting for data', c: '#9AA6B8' };
  const age = Date.now() - d.t;
  if (d.live && age < 90000) return { text: 'Live', c: '#3DD6A0' };
  const dt = new Date(d.t), today = dt.toDateString() === new Date().toDateString();
  return { text: 'As of ' + (today ? '' : dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) + ', ') + dt.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' }), c: age < 20 * 60000 ? '#E8B64A' : '#9AA6B8' };
}
function spark(arr, base, W, H) {
  if (!arr || arr.length < 2) return null;
  const all = num(base) ? arr.concat([base]) : arr, mn = Math.min(...all), mx = Math.max(...all), M = arr.length - 1;
  const y = (v) => H - 2 - (v - mn) / ((mx - mn) || 1) * (H - 4), x = (i) => 1 + i / M * (W - 2);
  return { p: arr.map((v, i) => x(i).toFixed(1) + ',' + y(v).toFixed(1)).join(' '), by: num(base) ? y(base).toFixed(1) : null, ex: x(M).toFixed(1), ey: y(arr[M]).toFixed(1) };
}
function rows() {
  return S.symbols.filter((s) => S.ui.mkt === 'All' || (S.ui.mkt === 'CAD') === isCA(s)).map((sym) => {
    const d = D(sym), q = d.q || {}, m = d.metric || {}, p = d.profile || {};
    const hi = num(m['52WeekHigh']), lo = num(m['52WeekLow']);
    return { sym, d, q, m, p, ok: !!d.q, name: p.name || sym, cur: p.currency || (isCA(sym) ? 'CAD' : 'USD'), ind: p.finnhubIndustry || '',
      pe: peOf(m), dy: num(m.dividendYieldIndicatedAnnual), hi, lo, pos: hi && lo && hi > lo && d.q ? Math.max(2, Math.min(97, (q.c - lo) / (hi - lo) * 100)) : null,
      cap: num(p.marketCapitalization), sc: score(d), fr: fresh(d) };
  });
}

/* ---------- views ---------- */
const seg = (label, key, opts) => '<div class="seg" role="group" aria-label="' + label + '">' + opts.map(([l, v]) => '<button data-act="ui" data-k="' + key + '" data-v="' + v + '" aria-pressed="' + (S.ui[key] === v) + '">' + l + '</button>').join('') + '</div>';
const needKey = () => (S.key ? '' : '<div class="notice"><div><b>Add your Finnhub API key to see live prices.</b><div class="sub">It stays in this browser. Each visitor uses their own key.</div></div><button class="btn pri" data-act="cfg">Open configuration</button></div>');
const freshHtml = (fr) => '<span class="fresh" style="color:' + fr.c + '"><span class="dot sm" style="background:' + fr.c + '"></span>' + fr.text + '</span>';
const stockHref = (sym) => '#/stock/' + encodeURIComponent(sym);

function viewHome() {
  const R = rows(), live = R.filter((r) => r.ok && num(r.q.dp) != null), n = R.length, mk = market();
  const W = 1300, B = 100; let p = 'M0,' + B;
  R.forEach((r, i) => { const cx = (i + 0.5) * W / n, a = Math.max(-82, Math.min(82, (num(r.q.dp) || 0) * 24));
    p += ' L' + (cx - 16).toFixed(1) + ',' + B + ' L' + (cx - 7).toFixed(1) + ',' + (B + a * 0.14).toFixed(1) + ' L' + cx.toFixed(1) + ',' + (B - a).toFixed(1) + ' L' + (cx + 7).toFixed(1) + ',' + (B + a * 0.3).toFixed(1) + ' L' + (cx + 15).toFixed(1) + ',' + B; });
  p += ' L' + W + ',' + B;
  const spikes = R.map((r) => '<a href="' + stockHref(r.sym) + '"><div class="mono" style="font-size:12px;font-weight:600;color:#fff">' + esc(short(r.sym)) + '</div><div class="mono ' + cls(r.q.dp) + '" style="margin-top:3px;font-size:11px">' + (r.ok ? sg(r.q.dp, 1) + '%' : '—') + '</div></a>').join('');
  const tapeItems = live.map((r) => '<span class="item mono"><span style="font-weight:600;color:#fff">' + esc(r.sym) + '</span><span style="color:#C5CEDB">' + f2(r.q.c) + '</span><span class="' + cls(r.q.dp) + '">' + sg(r.q.dp) + '%</span></span>').join('');
  const adv = live.filter((r) => r.q.dp > 0).length, dec = live.filter((r) => r.q.dp < 0).length, avg = live.length ? live.reduce((a, r) => a + r.q.dp, 0) / live.length : null;
  const sorted = [...live].sort((a, b) => b.q.dp - a.q.dp);
  const movers = (sorted.length > 6 ? sorted.slice(0, 3).concat(sorted.slice(-3)) : sorted).map((r) => '<a class="lrow" href="' + stockHref(r.sym) + '"><span><span class="mono" style="font-weight:600">' + esc(r.sym) + '</span><span style="margin-left:10px;color:#8793A6">' + esc(r.name) + '</span></span><span class="mono ' + cls(r.q.dp) + '" style="font-weight:600">' + sg(r.q.dp) + '%</span></a>').join('');
  const setups = R.filter((r) => r.sc).sort((a, b) => b.sc.sc - a.sc.sc).slice(0, 3).map((r) => {
    const best = [...r.sc.fs].sort((a, b) => b.v - a.v).filter((f) => f.v > 0).slice(0, 2).map((f) => f.t).join('. ');
    return '<div class="gcard" style="background:#0E1219;padding:16px"><div style="display:flex;justify-content:space-between;align-items:baseline;gap:8px"><a class="open mono sym" style="font-size:17px" href="' + stockHref(r.sym) + '">' + esc(r.sym) + '</a><span class="mono ' + cls(r.q.dp) + '" style="font-size:13px">' + sg(r.q.dp) + '%</span></div>'
      + '<div class="nm">' + esc(r.name) + '</div><div style="display:flex;align-items:baseline;gap:8px;margin-top:14px"><span class="mono c-' + r.sc.k + '" style="font-size:34px;font-weight:600">' + r.sc.sc + '</span><span style="font-size:12px;color:#9AA6B8">/ 100 entry score</span></div>'
      + '<div class="bar" style="margin-top:8px"><i class="fill ' + r.sc.k + '" style="width:' + r.sc.sc + '%"></i></div><p style="margin-top:12px;font-size:13px;line-height:1.5;color:#C5CEDB">' + esc(best ? best + '.' : 'Mixed signals.') + '</p></div>';
  }).join('');
  return '<section class="hero"><div class="wrap">' + needKey()
    + '<div class="lbl" style="color:#6AA8FF">' + esc((mk.day ? mk.day + ' · ' : '') + mk.text) + '</div>'
    + '<h1>' + n + ' stocks.<br>One pulse.</h1>'
    + '<p>Every spike on the line below is one of your stocks today. Up for a gain, down for a loss, taller for a bigger move. Pick one to see the detail behind it.</p>'
    + '<div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:26px"><a class="btn pri lg" href="#/watchlist">Open watchlist</a><a class="btn lg" href="#/heatmap">See the heatmap</a></div></div>'
    + '<div class="pulse-scroll"><div class="pulse-in"><svg viewBox="0 0 1300 190" preserveAspectRatio="none" fill="none" aria-hidden="true"><path d="' + p + '" stroke="#1F3350" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke"/><path class="run" pathLength="1" d="' + p + '" stroke="#6AA8FF" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/></svg>'
    + '<div class="spikes" style="grid-template-columns:repeat(' + Math.max(n, 1) + ',minmax(0,1fr))">' + spikes + '</div></div></div>'
    + (tapeItems ? '<div class="tapebox" aria-hidden="true"><div class="tape">' + tapeItems + tapeItems + '</div></div>' : '') + '</section>'
    + '<div class="wrap"><div class="tiles">'
    + '<div class="card"><div class="lbl">Breadth</div><div class="mono big">' + adv + ' up <span style="color:#9AA6B8">·</span> ' + dec + ' down</div><div style="display:flex;gap:3px;margin-top:12px"><div style="height:6px;border-radius:3px;background:#3DD6A0;width:' + (live.length ? adv / live.length * 100 : 50).toFixed(0) + '%"></div><div style="height:6px;border-radius:3px;background:#FF7A85;flex:1"></div></div></div>'
    + '<div class="card"><div class="lbl">Average move</div><div class="mono big ' + cls(avg) + '">' + (avg == null ? '—' : sg(avg) + '%') + '</div><div class="sub" style="font-size:13px">Equal-weighted, today</div></div>'
    + '<div class="card"><div class="lbl">Top mover</div><div class="mono big up">' + (sorted.length ? esc(sorted[0].sym) : '—') + '</div><div class="sub" style="font-size:13px">' + (sorted.length ? sg(sorted[0].q.dp) + '% · ' + esc(sorted[0].name) : '') + '</div></div>'
    + '<div class="card"><div class="lbl">Weakest</div><div class="mono big dn">' + (sorted.length ? esc(sorted[sorted.length - 1].sym) : '—') + '</div><div class="sub" style="font-size:13px">' + (sorted.length ? sg(sorted[sorted.length - 1].q.dp) + '% · ' + esc(sorted[sorted.length - 1].name) : '') + '</div></div>'
    + '</div><div class="cols3"><section class="card" style="flex:2 1 560px;min-width:0"><h2 class="lbl" style="margin-bottom:14px">Best entry scores right now</h2>' + (setups ? '<div class="setups">' + setups + '</div>' : '<div class="empty">Scores appear once prices and fundamentals have loaded.</div>') + '</section>'
    + '<section class="card" style="flex:1 1 320px;min-width:0"><h2 class="lbl" style="margin-bottom:8px">Biggest movers</h2>' + (movers || '<div class="empty">No prices yet.</div>') + '</section></div></div>';
}

const COLS = [['sym', 'Symbol', ''], ['c', 'Price', 'end'], ['dp', 'Chg %', 'end'], [null, 'Today', 'mid'], ['d', 'Chg', 'end'], ['pe', 'P/E', 'end'], ['dy', 'Yield', 'end'], ['pos', '52-wk range', ''], ['rec', 'Analysts', ''], ['sc', 'Good time to invest?', '']];
function sortVal(r, k) {
  if (k === 'sym') return r.sym;
  if (k === 'c' || k === 'dp' || k === 'd') return num(r.q[k]);
  if (k === 'rec') { const x = r.d.rec; if (!x) return null; const t = x.strongBuy + x.buy + x.hold + x.sell + x.strongSell; return t ? (x.strongBuy + x.buy) / t : null; }
  if (k === 'sc') return r.sc ? r.sc.sc : null;
  return num(r[k]);
}
function recHtml(x) {
  if (!x) return '<span class="fl" style="font-size:12px">—</span>';
  const b = x.strongBuy + x.buy, h = x.hold, s = x.sell + x.strongSell; if (!(b + h + s)) return '<span class="fl" style="font-size:12px">—</span>';
  return '<div><div class="recbar"><i style="flex:' + b + ';background:#3DD6A0"></i><i style="flex:' + h + ';background:#E8B64A"></i><i style="flex:' + s + ';background:#FF7A85"></i></div><div class="mono" style="margin-top:6px;font-size:11px;color:#9AA6B8">' + b + ' buy · ' + h + ' hold · ' + s + ' sell</div></div>';
}
function sparkSvg(r, W, H, grid) {
  const s = spark(r.d.ticks, r.q.pc, W, H); if (!s) return '<span class="fl">—</span>';
  const c = hex(r.q.dp);
  return grid
    ? '<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none" fill="none" role="img" aria-label="Today\'s price path" style="display:block;width:100%;height:' + H + 'px;margin-top:10px">' + (s.by ? '<line x1="0" x2="' + W + '" y1="' + s.by + '" y2="' + s.by + '" stroke="#3A4659" stroke-dasharray="2 3" vector-effect="non-scaling-stroke"/>' : '') + '<polyline points="' + s.p + '" stroke="' + c + '" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/></svg>'
    : '<svg width="' + W + '" height="' + H + '" viewBox="0 0 ' + W + ' ' + H + '" fill="none" role="img" aria-label="Today\'s price path" style="overflow:visible">' + (s.by ? '<line x1="1" x2="' + (W - 1) + '" y1="' + s.by + '" y2="' + s.by + '" stroke="#3A4659" stroke-dasharray="2 3"/>' : '') + '<polyline points="' + s.p + '" stroke="' + c + '" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/><circle cx="' + s.ex + '" cy="' + s.ey + '" r="2.6" fill="' + c + '"/></svg>';
}
function viewWatchlist() {
  const R = rows(), k = S.ui.sort, dir = S.ui.dir;
  R.sort((a, b) => { const x = sortVal(a, k), y = sortVal(b, k); if (x == null && y == null) return 0; if (x == null) return 1; if (y == null) return -1; return (typeof x === 'string' ? x.localeCompare(y) : x - y) * dir; });
  let body;
  if (S.ui.layout === 'table') {
    const head = COLS.map(([key, l, al]) => key ? '<button class="th ' + al + (key === k ? ' on' : '') + '" data-act="sort" data-k="' + key + '">' + l + (key === k ? '<span aria-hidden="true" style="font-size:9px">' + (dir > 0 ? '▲' : '▼') + '</span>' : '') + '</button>' : '<span class="th ' + al + '" style="cursor:default">' + l + '</span>').join('');
    const trs = R.map((r) => {
      const first = '<a class="open" href="' + stockHref(r.sym) + '"><div style="display:flex;align-items:baseline;gap:8px"><span class="mono sym">' + esc(r.sym) + '</span><span class="tag">' + esc(r.cur) + '</span></div><div class="nm">' + esc(r.name) + (r.ind ? ' · ' + esc(r.ind) : '') + '</div></a>';
      if (!r.ok) return '<div class="row"><div class="cols">' + first + '<div style="grid-column:2 / -1;color:#9AA6B8;font-size:13px">' + esc(r.d.unavailable || (S.key ? 'Loading…' : 'Add your API key to load prices')) + '</div></div></div>';
      const why = S.ui.why === r.sym && r.sc ? '<div class="why above"><div style="font-size:13px;color:#C5CEDB">Why ' + r.sc.sc + ' / 100 for ' + esc(r.sym) + ': starts at 50, then</div><div class="chips">' + r.sc.fs.map((f) => '<span class="chip"><span class="mono ' + (f.v > 0 ? 'up' : 'dn') + '" style="font-weight:600">' + (f.v > 0 ? '+' : '−') + Math.abs(f.v) + '</span>' + esc(f.t) + '</span>').join('') + '</div></div>' : '';
      return '<div class="row"><div class="cols">' + first
        + '<div class="r"><div class="mono" style="font-size:15px;font-weight:500">' + f2(r.q.c) + '</div><div style="margin-top:3px">' + freshHtml(r.fr) + '</div></div>'
        + '<div class="r mono ' + cls(r.q.dp) + '" style="font-weight:600">' + sg(r.q.dp) + '%</div>'
        + '<div style="display:flex;justify-content:center">' + sparkSvg(r, 104, 30, false) + '</div>'
        + '<div class="r mono ' + cls(r.q.d) + '">' + sg(r.q.d) + '</div>'
        + '<div class="r mono" style="font-size:13px;color:#C5CEDB">' + (r.pe != null && r.pe > 0 ? r.pe.toFixed(1) : '—') + '</div>'
        + '<div class="r mono" style="font-size:13px;color:#C5CEDB">' + (r.dy != null && r.dy > 0 ? r.dy.toFixed(1) + '%' : '—') + '</div>'
        + (r.pos != null ? '<div><div class="rng"><i style="left:' + r.pos.toFixed(0) + '%"></i></div><div class="rng-l mono"><span>' + f2(r.lo) + '</span><span>' + f2(r.hi) + '</span></div></div>' : '<div class="fl">—</div>')
        + recHtml(r.d.rec)
        + '<div class="above" style="display:flex;align-items:center;gap:10px"><div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:7px"><div>' + pill(r.sc, r.sym, true) + '</div><div class="bar" style="height:5px"><i class="fill ' + (r.sc ? r.sc.k : 'na') + '" style="width:' + (r.sc ? r.sc.sc : 0) + '%"></i></div></div><span class="mono c-' + (r.sc ? r.sc.k : 'na') + '" style="font-weight:600">' + (r.sc ? r.sc.sc : '') + '</span></div>'
        + '</div>' + why + '</div>';
    }).join('');
    body = '<div class="card tbl"><div class="tbl-in"><div class="cols thead">' + head + '</div>' + trs + '</div></div><p class="hint" style="margin-top:14px">Click a row for the detail page. Click a verdict to see how its score was built. The dot under each price shows how fresh it is.</p>';
  } else {
    body = '<div class="grid">' + R.map((r) => {
      const top = '<div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px"><a class="open mono sym" style="font-size:17px" href="' + stockHref(r.sym) + '">' + esc(r.sym) + '</a><span class="nm" style="margin:0">' + esc(r.name) + '</span></div>';
      if (!r.ok) return '<div class="gcard">' + top + '<p class="sub" style="margin-top:14px">' + esc(r.d.unavailable || (S.key ? 'Loading…' : 'Add your API key to load prices')) + '</p></div>';
      const dpos = num(r.q.h) && num(r.q.l) && r.q.h > r.q.l ? Math.max(3, Math.min(100, (r.q.c - r.q.l) / (r.q.h - r.q.l) * 100)) : null;
      return '<div class="gcard">' + top
        + '<div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-top:14px"><span class="mono" style="font-size:30px;font-weight:600;letter-spacing:-.02em">' + f2(r.q.c) + '<span style="margin-left:6px;font-size:12px;font-weight:500;color:#9AA6B8">' + esc(r.cur) + '</span></span><span class="mono ' + cls(r.q.dp) + '" style="font-size:22px;font-weight:600">' + sg(r.q.dp) + '%</span></div>'
        + '<div style="margin-top:6px">' + freshHtml(r.fr) + '</div>' + sparkSvg(r, 260, 40, true)
        + '<div style="margin-top:14px;padding-top:14px;border-top:1px solid #232C3A">'
        + '<div class="rl mono"><span>L ' + f2(r.q.l) + '</span><b>Today\'s range</b><span>H ' + f2(r.q.h) + '</span></div><div class="bar" style="margin-top:7px"><i style="background:linear-gradient(90deg,#1BA3C6,#3B82F6);width:' + (dpos == null ? 0 : dpos.toFixed(0)) + '%"></i></div>'
        + '<div class="rl mono" style="margin-top:12px"><span>L ' + f2(r.lo) + '</span><b>52-week range</b><span>H ' + f2(r.hi) + '</span></div><div class="bar" style="margin-top:7px"><i style="background:linear-gradient(90deg,#17A57A,#3DD6C8);width:' + (r.pos == null ? 0 : r.pos.toFixed(0)) + '%"></i></div></div>'
        + '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:16px">' + pill(r.sc, r.sym, false) + '<span class="mono c-' + (r.sc ? r.sc.k : 'na') + '" style="font-weight:600">' + (r.sc ? r.sc.sc + ' / 100' : '') + '</span></div></div>';
    }).join('') + '</div>';
  }
  return '<div class="wrap">' + needKey() + '<div class="top"><div><h1 class="h1">Watchlist</h1><p class="sub">' + R.length + ' names · prices in each stock\'s own currency</p></div><div class="ctrls">' + seg('Layout', 'layout', [['List', 'table'], ['Grid', 'grid']]) + seg('Market', 'mkt', [['All', 'All'], ['US', 'US'], ['CAD', 'CAD']]) + '</div></div>' + body + '</div>';
}

const FRAMES = { '1D': { label: 'Today', scale: 3, get: (r) => num(r.q.dp) }, '1W': { label: 'Past 5 days', scale: 6, get: (r) => num(r.m['5DayPriceReturnDaily']) }, '3M': { label: 'Past 13 weeks', scale: 20, get: (r) => num(r.m['13WeekPriceReturnDaily']) } };
function tint(v, scale) { const t = Math.min(Math.abs(v) / scale, 1), a = [35, 44, 58], b = v >= 0 ? [23, 116, 90] : [168, 50, 63]; return 'rgb(' + a.map((x, i) => Math.round(x + (b[i] - x) * t)).join(',') + ')'; }
function viewHeatmap() {
  const F = FRAMES[S.ui.frame] || FRAMES['1D'];
  const items = rows().map((r) => ({ r, v: r.ok ? F.get(r) : null })).filter((x) => x.v != null);
  const mxw = Math.max(1, ...items.map((x) => Math.sqrt(x.r.cap || 0)));
  items.forEach((x) => { x.w = S.ui.size === 'cap' ? Math.max(Math.sqrt(x.r.cap || 0), mxw * 0.3) : 1; });
  items.sort((a, b) => b.w - a.w);
  let map = '<div class="card empty">No data for this view yet. Prices and fundamentals load a few seconds after you add a key.</div>';
  if (items.length) {
    const Rn = items.length > 14 ? 4 : items.length > 8 ? 3 : items.length > 3 ? 2 : 1, rws = Array.from({ length: Rn }, () => ({ it: [], sum: 0 }));
    items.forEach((x) => { const r = rws.reduce((a, b) => (a.sum <= b.sum ? a : b)); r.it.push(x); r.sum += x.w; });
    rws.sort((a, b) => b.sum - a.sum); const total = rws.reduce((a, r) => a + r.sum, 0); let y = 0, tiles = '';
    rws.forEach((rw) => { const h = rw.sum / total * 100; let x = 0; rw.it.forEach(({ r, v, w }) => { const wp = w / rw.sum * 100, share = wp * h / 100, big = share >= 7;
      tiles += '<a class="tile" href="' + stockHref(r.sym) + '" aria-label="' + esc(r.name + ' ' + sg(v) + '%') + '" style="left:' + x.toFixed(3) + '%;top:' + y.toFixed(3) + '%;width:' + wp.toFixed(3) + '%;height:' + h.toFixed(3) + '%"><div style="background:' + tint(v, F.scale) + '"><div><div class="mono" style="font-weight:600;line-height:1.1;font-size:' + Math.round(13 + Math.sqrt(share) * 3.2) + 'px">' + esc(r.sym) + '</div>' + (big ? '<div style="margin-top:4px;font-size:13px;color:rgba(255,255,255,.82)">' + esc(r.name) + '</div>' : '') + '</div><div><div class="mono" style="font-weight:600;font-size:' + Math.round(13 + Math.sqrt(share) * 2) + 'px">' + sg(v) + '%</div>' + (big ? '<div class="mono" style="margin-top:2px;font-size:13px;color:rgba(255,255,255,.82)">' + f2(r.q.c) + ' ' + esc(r.cur) + '</div>' : '') + '</div></div></a>'; x += wp; }); y += h; });
    map = '<div class="heat">' + tiles + '</div><div class="legend"><div class="mono" style="display:flex;align-items:center;gap:10px;font-size:12px;color:#B7C1D1"><span>−' + F.scale + '%</span><div class="g"></div><span>+' + F.scale + '%</span></div><span class="hint" style="margin:0">' + (S.ui.size === 'cap' ? 'Tile area scales with the square root of market cap, with a minimum size so small names and funds stay readable.' : 'All tiles equal size.') + '</span></div>';
  }
  const so = [...items].sort((a, b) => b.v - a.v), mrow = (x) => '<a class="lrow" href="' + stockHref(x.r.sym) + '"><span><span class="mono" style="font-weight:600">' + esc(x.r.sym) + '</span><span style="margin-left:10px;color:#8793A6">' + esc(x.r.name) + '</span></span><span class="mono ' + cls(x.v) + '" style="font-weight:600">' + sg(x.v) + '%</span></a>';
  const half = Math.min(3, Math.floor(so.length / 2));
  return '<div class="wrap">' + needKey() + '<div class="top"><div><h1 class="h1">Heatmap</h1><p class="sub">' + F.label + ' · ' + items.length + ' names · click a tile for the detail page</p></div><div class="ctrls">' + seg('Performance window', 'frame', [['1D', '1D'], ['5D', '1W'], ['13W', '3M']]) + seg('Tile size', 'size', [['Market cap', 'cap'], ['Equal', 'eq']]) + seg('Market', 'mkt', [['All', 'All'], ['US', 'US'], ['CAD', 'CAD']]) + '</div></div>' + map
    + (half ? '<div class="tiles" style="margin-top:28px;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px"><section class="card"><h2 class="lbl" style="margin-bottom:8px">Leaders</h2>' + so.slice(0, half).map(mrow).join('') + '</section><section class="card"><h2 class="lbl" style="margin-bottom:8px">Laggards</h2>' + so.slice(-half).reverse().map(mrow).join('') + '</section></div>' : '') + '</div>';
}

function viewStock(sym) {
  if (!S.symbols.includes(sym)) return '<div class="wrap"><div class="card empty">' + esc(sym) + ' is not on your watchlist. Add it in Configuration.<div style="margin-top:14px"><a class="btn" style="display:inline-flex;align-items:center" href="#/watchlist">Back to watchlist</a></div></div></div>';
  const all = S.ui.mkt; S.ui.mkt = 'All'; const R = rows(); S.ui.mkt = all;
  const r = R.find((x) => x.sym === sym), q = r.q, m = r.m, sc = r.sc;
  fetchNews(sym);
  const chips = R.map((x) => '<a class="schip" href="' + stockHref(x.sym) + '"' + (x.sym === sym ? ' aria-current="true"' : '') + '><span class="mono" style="font-size:12px;font-weight:600;color:' + (x.sym === sym ? '#fff' : '#C5CEDB') + '">' + esc(short(x.sym)) + '</span><span class="mono ' + cls(x.q.dp) + '" style="font-size:11px">' + (x.ok ? sg(x.q.dp, 1) + '%' : '—') + '</span></a>').join('');
  const head = '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin-bottom:22px"><a class="btn" style="display:flex;align-items:center;gap:6px;padding:0 14px" href="#/watchlist"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>Watchlist</a><div class="chips" style="margin-left:8px">' + chips + '</div></div>';
  if (!r.ok) return '<div class="wrap" style="padding-top:20px">' + needKey() + head + '<div class="card empty"><b>' + esc(sym) + '</b><br>' + esc(r.d.unavailable || (S.key ? 'Loading price…' : 'Add your API key to load this stock.')) + '</div></div>';
  const s = spark(r.d.ticks, q.pc, 900, 260), c = hex(q.dp);
  const chart = s && r.d.ticks.length >= 3
    ? '<svg viewBox="0 0 900 260" preserveAspectRatio="none" role="img" aria-label="Price since this page was opened" style="display:block;width:100%;height:260px">' + (s.by ? '<line x1="0" x2="900" y1="' + s.by + '" y2="' + s.by + '" stroke="#4A5870" stroke-dasharray="3 4" vector-effect="non-scaling-stroke"/>' : '') + '<polygon points="' + s.p + ' 899,260 1,260" fill="' + c + '" fill-opacity=".10"/><polyline points="' + s.p + '" fill="none" stroke="' + c + '" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg><p class="hint">Built from live updates since you opened the page, starting from the previous close (dashed) and today\'s open. Finnhub\'s free plan does not include price history.</p>'
    : '<div class="empty">The chart fills in as live prices arrive while this page is open.</div>';
  const stat = (l, v) => '<div class="card"><div class="lbl" style="font-weight:500">' + l + '</div><div class="mono" style="margin-top:6px;font-size:16px;font-weight:500">' + v + '</div></div>';
  const stats = stat('Previous close', f2(q.pc)) + stat('Open', f2(q.o)) + stat('Day range', f2(q.l) + ' – ' + f2(q.h)) + stat('52-week range', f2(r.lo) + ' – ' + f2(r.hi)) + stat('Market cap', capStr(r.cap) + (r.cap ? ' ' + esc(r.cur) : '')) + stat('P/E (TTM)', r.pe != null && r.pe > 0 ? r.pe.toFixed(1) : '—') + stat('Dividend yield', r.dy != null && r.dy > 0 ? r.dy.toFixed(1) + '%' : 'None') + stat('Beta', num(m.beta) != null ? m.beta.toFixed(2) : '—');
  const perf = [['Today', num(q.dp)], ['5 days', num(m['5DayPriceReturnDaily'])], ['Month to date', num(m.monthToDatePriceReturnDaily)], ['13 weeks', num(m['13WeekPriceReturnDaily'])], ['26 weeks', num(m['26WeekPriceReturnDaily'])], ['Year to date', num(m.yearToDatePriceReturnDaily)], ['1 year', num(m['52WeekPriceReturnDaily'])]]
    .map(([l, v]) => '<div style="padding:12px 14px;background:#0E1219;border-radius:8px"><div style="font-size:12px;color:#9AA6B8">' + l + '</div><div class="mono ' + cls(v) + '" style="margin-top:4px;font-size:16px;font-weight:600">' + (v == null ? '—' : sg(v) + '%') + '</div></div>').join('');
  const n = S.news[sym], items = n && n.items;
  const news = !items ? '<div class="empty">Loading news…</div>' : !items.length ? '<div class="empty">No headlines from Finnhub in the past week.</div>' : '<div class="news">' + items.filter((a) => /^https?:\/\//.test(a.url || '')).map((a) => '<a href="' + esc(a.url) + '" target="_blank" rel="noopener noreferrer"><div class="hl">' + esc(a.headline) + '</div><div class="src">' + esc(a.source || '') + ' · ' + new Date((a.datetime || 0) * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) + '</div></a>').join('') + '</div>';
  const x = r.d.rec, tot = x ? x.strongBuy + x.buy + x.hold + x.sell + x.strongSell : 0;
  const analysts = tot ? '<div class="recbar" style="height:8px"><i style="flex:' + (x.strongBuy + x.buy) + ';background:#3DD6A0"></i><i style="flex:' + x.hold + ';background:#E8B64A"></i><i style="flex:' + (x.sell + x.strongSell) + ';background:#FF7A85"></i></div><div style="display:flex;justify-content:space-between;margin-top:8px;font-size:13px"><span class="up">' + (x.strongBuy + x.buy) + ' buy</span><span style="color:#E8B64A">' + x.hold + ' hold</span><span class="dn">' + (x.sell + x.strongSell) + ' sell</span></div><p class="hint">Latest monthly tally from Finnhub' + (x.period ? ' (' + esc(x.period) + ')' : '') + '.</p>' : '<div class="empty" style="padding:12px">No analyst ratings available.</div>';
  const scoreCard = sc ? '<div style="display:flex;align-items:baseline;gap:8px;margin-top:12px"><span class="mono c-' + sc.k + '" style="font-size:36px;font-weight:600">' + sc.sc + '</span><span style="font-size:13px;color:#9AA6B8">/ 100 · starts at 50</span></div><div class="bar" style="margin:8px 0 10px"><i class="fill ' + sc.k + '" style="width:' + sc.sc + '%"></i></div>' + sc.fs.map((f) => '<div class="line"><span style="color:#C5CEDB">' + esc(f.t) + '</span><span class="mono ' + (f.v > 0 ? 'up' : 'dn') + '" style="font-weight:600">' + (f.v > 0 ? '+' : '−') + Math.abs(f.v) + '</span></div>').join('') + '<p class="hint">A rule-based estimate from analyst ratings, trend and valuation. It is not a prediction and not investment advice.</p>' : '<div class="empty" style="padding:12px">Not enough data yet to score this one.</div>';
  return '<div class="wrap" style="padding-top:20px">' + head
    + '<div class="top" style="margin-bottom:24px"><div><div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap"><h1 class="h1" style="font-size:30px">' + esc(r.name) + '</h1><span class="mono tag" style="font-size:14px;padding:3px 8px">' + esc(sym) + '</span><span style="font-size:13px;color:#9AA6B8">' + esc(r.p.exchange || '') + '</span></div><div style="display:flex;align-items:center;gap:12px;margin-top:8px;color:#9AA6B8">' + esc(r.ind) + freshHtml(r.fr) + '</div></div>'
    + '<div style="display:flex;align-items:baseline;gap:14px"><span class="mono" style="font-size:38px;font-weight:600;letter-spacing:-.02em">' + f2(q.c) + '</span><span style="font-size:13px;color:#9AA6B8">' + esc(r.cur) + '</span><span class="mono ' + cls(q.dp) + '" style="font-size:18px;font-weight:600">' + sg(q.d) + '  (' + sg(q.dp) + '%)</span></div></div>'
    + '<div class="two"><div class="main"><section class="card"><h2 class="lbl" style="margin-bottom:14px">Today</h2>' + chart + '</section><div class="stats">' + stats + '</div>'
    + '<section class="card"><h2 class="lbl" style="margin-bottom:14px">Performance</h2><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px">' + perf + '</div></section>'
    + '<section class="card"><h2 class="lbl" style="margin-bottom:4px">News · past week</h2>' + news + '</section></div>'
    + '<aside class="side"><section class="card"><div style="display:flex;align-items:center;justify-content:space-between;gap:10px"><h2 class="lbl">Good time to invest?</h2>' + pill(sc, sym, false) + '</div>' + scoreCard + '</section>'
    + '<section class="card"><h2 class="lbl" style="margin-bottom:12px">Analyst consensus</h2>' + analysts + '</section></aside></div></div>';
}

/* ---------- render loop ---------- */
function route() {
  const h = location.hash.replace(/^#\/?/, ''), parts = h.split('/');
  if (parts[0] === 'watchlist') return { name: 'watchlist' };
  if (parts[0] === 'heatmap') return { name: 'heatmap' };
  if (parts[0] === 'stock' && parts[1]) return { name: 'stock', sym: cleanSymbols(decodeURIComponent(parts[1]))[0] || '' };
  return { name: 'home' };
}
function render() {
  dirty = false;
  const r = route(), app = $('#app');
  app.innerHTML = r.name === 'watchlist' ? viewWatchlist() : r.name === 'heatmap' ? viewHeatmap() : r.name === 'stock' ? viewStock(r.sym) : viewHome();
  document.querySelectorAll('.nav a').forEach((a) => { const on = a.dataset.nav === r.name || (a.dataset.nav === 'watchlist' && r.name === 'stock'); if (on) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current'); });
  const mk = market(); $('#mkt-text').textContent = mk.text; $('#mkt-dot').style.background = mk.c;
  document.title = (r.name === 'stock' ? r.sym + ' · ' : '') + 'StockPulse';
}
function renderLog() {
  const box = $('#log'); if (!box) return;
  const lv = S.ui.lvl, list = S.log.filter((e) => lv === 'All' || e.lvl === lv.toUpperCase());
  const atEnd = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  box.innerHTML = list.length ? list.map((e) => '<div class="log-row"><span class="t">' + e.t + '</span><span class="lv-' + e.lvl + '">' + e.lvl + '</span><span class="m">' + esc(e.msg) + '</span></div>').join('') : '<div class="empty" style="font-family:Inter,sans-serif">No entries. Feed events will appear here.</div>';
  if (atEnd) box.scrollTop = box.scrollHeight;
  $('#log-levels').innerHTML = ['All', 'Info', 'Warn', 'Error'].map((k) => '<button data-act="lvl" data-v="' + k + '" aria-pressed="' + (lv === k) + '">' + k + ' ' + S.log.filter((e) => k === 'All' || e.lvl === k.toUpperCase()).length + '</button>').join('');
}

/* ---------- start / restart the feed ---------- */
function start() {
  queue = []; S.authError = false; S.pauseUntil = 0; S.lastOk = 0; S.data = {}; S.news = {}; wsRetry = 0;
  if (ws) { try { ws.onclose = null; ws.close(); } catch (e) {} ws = null; S.wsState = 'closed'; }
  updateStatus(); dirty = true;
  if (!S.key) { log('WARN', 'No Finnhub API key set · open Configuration to add one'); return; }
  loadAll(); wsConnect();
}

/* ---------- events ---------- */
function toggleCfg(open) {
  const el = $('#cfg'), show = open == null ? el.hidden : open;
  el.hidden = !show; $('#cfg-btn').setAttribute('aria-expanded', String(show));
  if (show) { $('#fh-key').value = S.key; $('#syms').value = S.symbols.join(', '); renderLog(); window.scrollTo(0, 0); }
}
document.addEventListener('click', (ev) => {
  const el = ev.target.closest('[data-act]'); if (!el) return;
  const act = el.dataset.act;
  if (act === 'ui') { S.ui[el.dataset.k] = el.dataset.v; saveUi(); render(); }
  else if (act === 'sort') { const k = el.dataset.k; if (S.ui.sort === k) S.ui.dir = -S.ui.dir; else { S.ui.sort = k; S.ui.dir = k === 'sym' ? 1 : -1; } saveUi(); render(); }
  else if (act === 'why') { ev.preventDefault(); S.ui.why = S.ui.why === el.dataset.sym ? null : el.dataset.sym; render(); }
  else if (act === 'cfg') toggleCfg(true);
  else if (act === 'lvl') { S.ui.lvl = el.dataset.v; saveUi(); renderLog(); }
});
$('#cfg-btn').addEventListener('click', () => toggleCfg());
$('#key-show').addEventListener('click', (e) => { const i = $('#fh-key'), show = i.type === 'password'; i.type = show ? 'text' : 'password'; e.target.textContent = show ? 'Hide' : 'Show'; });
$('#key-save').addEventListener('click', () => { S.key = $('#fh-key').value.trim(); LS.set('sp.key', S.key); log('INFO', S.key ? 'API key saved in this browser' : 'API key cleared'); start(); });
$('#key-clear').addEventListener('click', () => { S.key = ''; LS.del('sp.key'); $('#fh-key').value = ''; log('INFO', 'API key removed from this browser'); start(); });
$('#key-test').addEventListener('click', async () => {
  const k = $('#fh-key').value.trim(); if (!k) { log('WARN', 'Enter a key before testing'); return; }
  log('INFO', 'Testing the key with a quote request for AAPL');
  try { const r = await fetch(API + '/quote?symbol=AAPL&token=' + encodeURIComponent(k)); const j = r.ok ? await r.json() : null;
    if (r.ok && j && num(j.c)) log('INFO', 'Test passed · AAPL ' + f2(j.c)); else log('ERROR', 'Test failed · HTTP ' + r.status + (r.status === 401 ? ' (key not accepted)' : r.status === 429 ? ' (rate limit)' : ''));
  } catch (e) { log('ERROR', 'Test failed · could not reach finnhub.io'); }
});
$('#syms-save').addEventListener('click', () => { const list = cleanSymbols($('#syms').value); if (!list.length) { log('WARN', 'The symbol list is empty'); return; } S.symbols = list; LS.set('sp.symbols', list); $('#syms').value = list.join(', '); log('INFO', 'Watchlist saved · ' + list.length + ' symbols'); start(); });
$('#syms-reset').addEventListener('click', () => { S.symbols = DEFAULT_SYMBOLS.slice(); LS.set('sp.symbols', S.symbols); $('#syms').value = S.symbols.join(', '); log('INFO', 'Watchlist reset to the default list'); start(); });
$('#log-clear').addEventListener('click', () => { S.log = []; renderLog(); });
window.addEventListener('hashchange', () => { render(); window.scrollTo(0, 0); });

setInterval(pump, CALL_GAP);
setInterval(refreshNext, 3500);
setInterval(() => { updateStatus(); if (dirty) render(); }, 1000);
setInterval(() => { dirty = true; }, 30000);   // keep freshness labels and the market clock current

S.symbols = cleanSymbols(S.symbols.join(',')); if (!S.symbols.length) S.symbols = DEFAULT_SYMBOLS.slice();
render(); start();
})();

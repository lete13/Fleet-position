require('dotenv').config();
const express = require('express');
const http    = require('http');
const fs      = require('fs');
const path    = require('path');
const { WebSocketServer, WebSocket } = require('ws');

/* ── Config ─────────────────────────────────────── */
const num = (v, d) => (Number.isFinite(Number(v)) && v !== '' && v != null ? Number(v) : d);

const PORT       = process.env.PORT || 3000;
const AIS_KEY    = process.env.AIS_API_KEY || '';
const AIS_URL    = process.env.AIS_URL || 'wss://stream.aisstream.io/v0/stream';
const DATA_DIR   = process.env.DATA_DIR || process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const EDIT_TOKEN = process.env.EDIT_TOKEN || '';

const AIS_PING_MS    = num(process.env.AIS_PING_SEC, 30) * 1000;          // liveness probe
const AIS_RECYCLE_MS = num(process.env.AIS_RECYCLE_HOURS, 6) * 3600_000;  // forced reconnect

const FALLBACK          = (process.env.FALLBACK_PROVIDER || '').toLowerCase(); // vesselfinder | myshiptracking
const FALLBACK_KEY      = process.env.FALLBACK_API_KEY || '';
const FALLBACK_POLL_MS  = num(process.env.FALLBACK_POLL_MIN, 60) * 60_000;
const FALLBACK_STALE_MS = num(process.env.FALLBACK_STALE_MIN, 60) * 60_000;

const STATUS_MS    = 60_000;   // status heartbeat to browsers
const TRACK_MAX    = 400;      // points kept per vessel
const TRACK_MIN_MS = 10 * 60_000;
const TRACK_MIN_NM = 2;

/* ── Fleet definition (fleet.json) ───────────────── */
const validMmsi = m => /^\d{9}$/.test(String(m || '')) && !/^\d{3}0{6}$/.test(String(m));

function loadFleet() {
  const raw = process.env.FLEET_JSON || fs.readFileSync(path.join(__dirname, 'fleet.json'), 'utf8');
  const fleet = JSON.parse(raw).map(v => ({
    id:   String(v.id),
    name: v.name,
    type: v.type || '',
    mmsi: validMmsi(v.mmsi) ? String(v.mmsi) : null,
    imo:  v.imo ? String(v.imo) : null,
  }));
  // MMSI_OVERRIDES=harilaos-junior:538012345,other-id:538000001
  (process.env.MMSI_OVERRIDES || '').split(',').map(s => s.trim()).filter(Boolean).forEach(pair => {
    const [id, mmsi] = pair.split(':').map(s => s.trim());
    const v = fleet.find(f => f.id === id);
    if (v && validMmsi(mmsi)) v.mmsi = mmsi;
    else console.warn(`[fleet] Ignored MMSI override "${pair}"`);
  });
  return fleet;
}

const FLEET = loadFleet();

/* ── State: last known fix per vessel, persisted to disk ── */
const DYNAMIC = ['lat','lng','sog','cog','hdg','ns','posTs','rxTs','src','dest','eta','etb','etc','bod','fw','track'];
const state = {};
FLEET.forEach(v => {
  state[v.id] = { ...v, lat:null, lng:null, sog:null, cog:null, hdg:null, ns:null,
                  posTs:null, rxTs:null, src:null, dest:null, eta:null,
                  etb:'', etc:'', bod:'', fw:'', track:[] };
});

function loadState() {
  try {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    let n = 0;
    for (const [id, s] of Object.entries(saved.vessels || {})) {
      const v = state[id];
      if (!v) continue;
      DYNAMIC.forEach(k => { if (s[k] !== undefined) v[k] = s[k]; });
      if (!v.mmsi && validMmsi(s.mmsi)) v.mmsi = s.mmsi;   // MMSI learned from a fallback source
      if (v.posTs) n++;
    }
    console.log(`[state] Restored last known positions for ${n}/${FLEET.length} vessels (saved ${saved.savedAt})`);
  } catch (e) {
    if (e.code !== 'ENOENT') console.warn('[state] Could not read saved state:', e.message);
    else console.log('[state] No saved state yet');
  }
}

let saveTimer = null;
function saveSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; saveNow(); }, 5000);
}
function saveNow() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = STATE_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ version: 1, savedAt: new Date().toISOString(), vessels: state }));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) {
    console.error('[state] Save failed:', e.message);
  }
}

/* ── Helpers ─────────────────────────────────────── */
function distNm(lat1, lng1, lat2, lng2) {
  const R = 3440.065, r = Math.PI / 180;
  const a = Math.sin((lat2 - lat1) * r / 2) ** 2 +
            Math.cos(lat1 * r) * Math.cos(lat2 * r) * Math.sin((lng2 - lng1) * r / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

const okPos = (lat, lng) =>
  Number.isFinite(lat) && Number.isFinite(lng) &&
  Math.abs(lat) <= 90 && Math.abs(lng) <= 180 && !(lat === 0 && lng === 0);

/* aisstream: "2026-11-09 21:38:02.318353 +0000 UTC" — also accepts ISO strings */
function parseTime(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2})?)/);
  const t = m ? Date.parse(`${m[1]}T${m[2]}Z`) : Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

/* AIS static ETA {Month,Day,Hour,Minute} (no year) → "YYYY-MM-DDTHH:MM" UTC */
function aisEta(e) {
  if (!e || typeof e !== 'object') return null;
  const { Month: mo, Day: d, Hour: h, Minute: mi } = e;
  if (!mo || !d || mo > 12 || d > 31 || h == null || h > 23 || mi == null || mi > 59) return null;
  const now = new Date();
  let y = now.getUTCFullYear();
  if (Date.UTC(y, mo - 1, d, h, mi) < now.getTime() - 30 * 86400_000) y++;
  const p = n => String(n).padStart(2, '0');
  return `${y}-${p(mo)}-${p(d)}T${p(h)}:${p(mi)}`;
}

const publicVessel = (v, withTrack) => {
  const { track, ...rest } = v;
  return withTrack ? { ...rest, track } : rest;
};
const snapshot = () => Object.values(state).map(v => publicVessel(v, true));

/* ── Single entry point for every position fix ───── */
function applyFix(v, fix) {
  const lat = Number(fix.lat), lng = Number(fix.lng);
  if (!okPos(lat, lng)) return false;

  const now = Date.now();
  let t = Number.isFinite(fix.ts) ? fix.ts : now;
  if (t > now + 5 * 60_000) t = now;                                   // clock skew at the source
  const prev = v.posTs ? Date.parse(v.posTs) : 0;
  if (t < prev) return false;                                          // older than what we hold

  v.lat = +lat.toFixed(5);
  v.lng = +lng.toFixed(5);
  v.posTs = new Date(t).toISOString();
  v.rxTs  = new Date(now).toISOString();
  v.src   = fix.src;
  // A fix that carries no speed/course must not keep the previous ones alive
  v.sog = Number.isFinite(fix.sog) && fix.sog < 102.3 ? +Number(fix.sog).toFixed(1) : null;
  v.cog = Number.isFinite(fix.cog) && fix.cog < 360   ? +Number(fix.cog).toFixed(1) : null;
  v.hdg = Number.isFinite(fix.hdg) && fix.hdg < 360   ? Math.round(fix.hdg) : null;
  if (Number.isInteger(fix.ns) && fix.ns >= 0 && fix.ns <= 15) v.ns = fix.ns;
  else if (fix.src === 'manual') v.ns = null;

  const last = v.track[v.track.length - 1];
  if (!last || t - last[0] >= TRACK_MIN_MS || distNm(last[1], last[2], v.lat, v.lng) >= TRACK_MIN_NM) {
    v.track.push([t, v.lat, v.lng]);
    if (v.track.length > TRACK_MAX) v.track.splice(0, v.track.length - TRACK_MAX);
  }
  saveSoon();
  broadcast({ type: 'update', vessel: publicVessel(v, false) });
  return true;
}

/* ── Express + HTTP server ───────────────────────── */
const app    = express();
const server = http.createServer(app);

app.use(express.json({ limit: '50kb' }));

const sourceStatus = () => ({
  ais: {
    configured:  !!AIS_KEY,
    online:      aisSocket?.readyState === WebSocket.OPEN && ais.subscribed,
    connectedAt: ais.connectedAt,
    lastMsgAt:   ais.lastMsgAt,
    messages:    ais.messages,
    reconnects:  ais.reconnects,
    lastError:   ais.lastError,
  },
  fallback: {
    provider:   FALLBACK || null,
    configured: !!(FALLBACK && FALLBACK_KEY && PROVIDERS[FALLBACK]),
    lastPollAt: fb.lastPollAt,
    lastError:  fb.lastError,
  },
  editProtected: !!EDIT_TOKEN,
});

app.get('/api/vessels', (_req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({ ok: true, ts: new Date().toISOString(), sources: sourceStatus(), vessels: snapshot() });
});

app.get('/api/health', (_req, res) => {
  const all = Object.values(state);
  const ages = all.filter(v => v.posTs).map(v => ({
    id: v.id, ageMin: Math.round((Date.now() - Date.parse(v.posTs)) / 60000), src: v.src,
  }));
  res.set('Cache-Control', 'no-store');
  res.json({
    ok: true,
    total: FLEET.length,
    withPosition: ages.length,
    noMmsi: all.filter(v => !v.mmsi).map(v => v.id),
    positions: ages,
    sources: sourceStatus(),
  });
});

/* Update voyage data (ETB / ETC / BOD / FW) and/or enter a position by hand (noon report) */
app.patch('/api/vessels/:id', (req, res) => {
  if (EDIT_TOKEN && req.get('x-edit-token') !== EDIT_TOKEN)
    return res.status(401).json({ ok: false, error: 'Edit token required' });

  const v = state[req.params.id] || Object.values(state).find(x => x.mmsi === req.params.id);
  if (!v) return res.status(404).json({ ok: false, error: 'Vessel not found' });

  const b = req.body || {};
  ['etb', 'etc', 'bod', 'fw'].forEach(k => {
    if (b[k] !== undefined) v[k] = String(b[k] ?? '').slice(0, 40);
  });

  let positionApplied;
  if (b.lat !== undefined || b.lng !== undefined) {
    const lat = Number(b.lat), lng = Number(b.lng);
    if (b.lat === '' || b.lng === '' || b.lat == null || b.lng == null || !okPos(lat, lng))
      return res.status(400).json({ ok: false, error: 'Invalid latitude/longitude' });
    const ts = b.posTs ? parseTime(b.posTs) : Date.now();
    if (ts == null) return res.status(400).json({ ok: false, error: 'Invalid position time' });
    positionApplied = applyFix(v, {
      lat, lng, ts, src: 'manual',
      sog: b.sog === '' || b.sog == null ? NaN : Number(b.sog),
      cog: b.cog === '' || b.cog == null ? NaN : Number(b.cog),
    });
    if (!positionApplied)
      return res.status(409).json({ ok: false, error: 'A newer position is already on file for this vessel' });
  }

  saveSoon();
  if (!positionApplied) broadcast({ type: 'update', vessel: publicVessel(v, false) });
  res.json({ ok: true, vessel: publicVessel(v, true) });
});

/* The page is one self-contained file; nothing else in the repo folder is served */
app.get('*', (_req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, 'index.html'));
});

/* ── Browser WebSocket (/ws) ─────────────────────── */
const wss     = new WebSocketServer({ server, path: '/ws' });
const clients = new Set();

wss.on('connection', ws => {
  clients.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  safe(ws, JSON.stringify({ type: 'snapshot', ts: new Date().toISOString(), sources: sourceStatus(), vessels: snapshot() }));
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function safe(ws, data) {
  try { if (ws.readyState === WebSocket.OPEN) ws.send(data); } catch {}
}
function broadcast(payload) {
  if (!clients.size) return;
  const msg = JSON.stringify(payload);
  clients.forEach(ws => safe(ws, msg));
}

/* Keeps proxies from idling the socket out, drops dead browsers, pushes source status */
setInterval(() => {
  clients.forEach(ws => {
    if (!ws.isAlive) { clients.delete(ws); return ws.terminate(); }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
  broadcast({ type: 'status', ts: new Date().toISOString(), sources: sourceStatus() });
}, STATUS_MS).unref();

/* ── Source 1: aisstream.io (terrestrial AIS, streaming) ── */
let aisSocket = null;
const ais = { subscribed: false, connectedAt: null, lastMsgAt: null, messages: 0, reconnects: 0,
              lastError: null, backoff: 2000, retryTimer: null, pingTimer: null, pongTimer: null, recycleTimer: null };

const aisMmsis = () => Object.values(state).filter(v => v.mmsi).map(v => v.mmsi);
const byMmsi   = mmsi => Object.values(state).find(v => v.mmsi === mmsi);

function scheduleAIS() {
  clearTimeout(ais.retryTimer);
  const wait = Math.round(ais.backoff * (0.75 + Math.random() * 0.5));
  ais.backoff = Math.min(ais.backoff * 2, 60_000);
  console.log(`[AIS] Reconnecting in ${(wait / 1000).toFixed(0)} s`);
  ais.retryTimer = setTimeout(connectAIS, wait);
}

function connectAIS() {
  if (!AIS_KEY) { console.log('[AIS] AIS_API_KEY not set — stream disabled'); return; }
  if (!aisMmsis().length) { console.log('[AIS] No vessel has an MMSI — stream disabled'); return; }
  if (aisSocket && aisSocket.readyState <= WebSocket.OPEN) return;

  console.log('[AIS] Connecting…');
  const sock = new WebSocket(AIS_URL, { perMessageDeflate: true, handshakeTimeout: 15_000 });
  aisSocket = sock;

  sock.on('open', () => {
    ais.connectedAt = new Date().toISOString();
    ais.subscribed = true;
    // Must be sent within 3 s of connecting or aisstream closes the socket
    sock.send(JSON.stringify({
      APIKey:             AIS_KEY,
      BoundingBoxes:      [[[-90, -180], [90, 180]]],
      FiltersShipMMSI:    aisMmsis(),
      FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport',
                           'LongRangeAisBroadcastMessage', 'ShipStaticData'],
    }));
    console.log(`[AIS] Connected ✓ — subscribed to ${aisMmsis().length} MMSI`);

    // A filtered feed for six ships is quiet for long stretches, so silence proves nothing.
    // Ping/pong tells a dead socket from a quiet one.
    ais.pingTimer = setInterval(() => {
      if (sock.readyState !== WebSocket.OPEN) return;
      if (!ais.pongTimer) ais.pongTimer = setTimeout(() => {
        console.warn('[AIS] No pong — connection is dead, recycling');
        ais.lastError = 'Connection stalled (no pong)';
        sock.terminate();
      }, 10_000);
      try { sock.ping(); } catch {}
    }, AIS_PING_MS);

    ais.recycleTimer = setTimeout(() => {
      console.log('[AIS] Scheduled recycle');
      sock.close(1000);
    }, AIS_RECYCLE_MS);
  });

  sock.on('pong', () => { clearTimeout(ais.pongTimer); ais.pongTimer = null; });

  sock.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.error || msg.Error) {
      ais.lastError = String(msg.error || msg.Error);
      ais.subscribed = false;
      ais.backoff = Math.max(ais.backoff, 30_000);   // bad key / throttled: do not hammer
      console.error('[AIS] Rejected:', ais.lastError);
      return;
    }
    ais.backoff = 2000;
    ais.lastError = null;
    ais.lastMsgAt = new Date().toISOString();
    ais.messages++;
    try { handleAIS(msg); } catch (e) { console.error('[AIS] Bad message:', e.message); }
  });

  sock.on('close', code => {
    clearInterval(ais.pingTimer); clearTimeout(ais.pongTimer); clearTimeout(ais.recycleTimer);
    ais.pongTimer = null;
    ais.subscribed = false;
    if (aisSocket !== sock) return;           // replaced on purpose
    ais.reconnects++;
    console.log(`[AIS] Disconnected (${code})`);
    if (code === 1000) ais.backoff = 2000;
    scheduleAIS();
  });

  sock.on('error', err => {
    ais.lastError = err.message;
    console.error('[AIS] Error:', err.message);
  });
}

function handleAIS(msg) {
  const meta = msg.MetaData || {};
  const mmsi = String(meta.MMSI_String || meta.MMSI || '');
  const v = byMmsi(mmsi);
  if (!v) return;

  const m   = msg.Message || {};
  const pos = m.PositionReport || m.StandardClassBPositionReport ||
              m.ExtendedClassBPositionReport || m.LongRangeAisBroadcastMessage;

  if (pos) {
    // Body first; MetaData key casing has differed between aisstream versions
    const lat = pos.Latitude  ?? meta.latitude  ?? meta.Latitude;
    const lng = pos.Longitude ?? meta.longitude ?? meta.Longitude;
    applyFix(v, {
      lat, lng,
      ts:  parseTime(meta.time_utc || meta.Time_utc || meta.TimeUtc) ?? Date.now(),
      src: 'ais',
      sog: pos.Sog, cog: pos.Cog,
      hdg: pos.TrueHeading,
      ns:  pos.NavigationalStatus,
    });
    return;
  }

  // Static/voyage data says nothing about where the ship is: never touches the position clock
  const stat = m.ShipStaticData;
  if (stat) {
    let changed = false;
    const dest = (stat.Destination || '').replace(/@+/g, '').trim();
    if (dest && dest !== v.dest) { v.dest = dest; changed = true; }
    const eta = aisEta(stat.Eta);
    if (eta && eta !== v.eta) { v.eta = eta; changed = true; }
    if (!v.imo && stat.ImoNumber) { v.imo = String(stat.ImoNumber); changed = true; }
    if (changed) { saveSoon(); broadcast({ type: 'update', vessel: publicVessel(v, false) }); }
  }
}

/* ── Source 2 (optional): REST fallback, polled only for vessels gone quiet ── */
const fb = { lastPollAt: null, lastError: null, running: false };

async function getJson(url, headers) {
  const r = await fetch(url, { headers, signal: AbortSignal.timeout(20_000) });
  const text = await r.text();
  if (!r.ok) throw new Error(`HTTP ${r.status} ${text.slice(0, 120)}`);
  return JSON.parse(text);
}

const PROVIDERS = {
  /* https://api.vesselfinder.com/docs/vessels.html — billed per record returned */
  async vesselfinder(vessels) {
    const out = [];
    const q = [];
    const mm = vessels.filter(v => v.mmsi).map(v => v.mmsi);
    const im = vessels.filter(v => !v.mmsi && v.imo).map(v => v.imo);
    if (mm.length) q.push(`mmsi=${mm.join(',')}`);
    if (im.length) q.push(`imo=${im.join(',')}`);
    if (!q.length) return out;
    const data = await getJson(`https://api.vesselfinder.com/vessels?userkey=${encodeURIComponent(FALLBACK_KEY)}&${q.join('&')}`);
    if (!Array.isArray(data)) throw new Error(String(data?.error || JSON.stringify(data).slice(0, 120)));
    for (const row of data) {
      const a = row.AIS || {};
      out.push({
        mmsi: a.MMSI != null ? String(a.MMSI) : null, imo: a.IMO ? String(a.IMO) : null,
        lat: Number(a.LATITUDE), lng: Number(a.LONGITUDE), ts: parseTime(a.TIMESTAMP),
        sog: Number(a.SPEED), cog: Number(a.COURSE), hdg: Number(a.HEADING), ns: Number(a.NAVSTAT),
        dest: a.DESTINATION, eta: a.ETA,
      });
    }
    return out;
  },

  /* https://api.myshiptracking.com/docs/vessel-current-position-api — one call per vessel */
  async myshiptracking(vessels) {
    const out = [];
    for (const v of vessels) {
      const id = v.mmsi ? `mmsi=${v.mmsi}` : v.imo ? `imo=${v.imo}` : null;
      if (!id) continue;
      try {
        const j = await getJson(`https://api.myshiptracking.com/api/v2/vessel?${id}&response=simple`,
                                { 'x-api-key': FALLBACK_KEY });
        const d = j.data;
        if (!d) continue;
        out.push({
          mmsi: d.mmsi != null ? String(d.mmsi) : null, imo: d.imo ? String(d.imo) : null,
          lat: Number(d.lat), lng: Number(d.lng), ts: parseTime(d.received),
          sog: Number(d.speed), cog: Number(d.course), hdg: NaN, ns: Number(d.nav_status),
          dest: d.destination, eta: d.eta,
        });
      } catch (e) { fb.lastError = `${v.id}: ${e.message}`; }
    }
    return out;
  },
};

async function pollFallback() {
  const provider = PROVIDERS[FALLBACK];
  if (!provider || !FALLBACK_KEY || fb.running) return;
  const now = Date.now();
  const due = Object.values(state).filter(v =>
    (v.mmsi || v.imo) && (!v.posTs || now - Date.parse(v.posTs) > FALLBACK_STALE_MS));
  if (!due.length) return;

  fb.running = true;
  try {
    fb.lastError = null;
    const rows = await provider(due);
    fb.lastPollAt = new Date().toISOString();
    let applied = 0, learned = false;
    for (const r of rows) {
      const v = due.find(x => (r.mmsi && x.mmsi === r.mmsi) || (r.imo && x.imo === r.imo));
      if (!v) continue;
      if (!v.mmsi && validMmsi(r.mmsi)) {
        v.mmsi = r.mmsi; learned = true;
        console.log(`[fallback] Learned MMSI ${r.mmsi} for ${v.name}`);
      }
      const dest = (r.dest || '').trim();
      if (dest) v.dest = dest;
      const eta = String(r.eta || '').match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})/);
      if (eta) v.eta = `${eta[1]}T${eta[2]}`;
      if (r.ts != null && applyFix(v, { ...r, src: FALLBACK })) applied++;
    }
    console.log(`[fallback] ${FALLBACK}: asked for ${due.length}, got ${rows.length}, newer fixes ${applied}`);
    if (learned) {                      // pick the vessel up on the live stream too
      saveSoon();
      if (aisSocket) { const s = aisSocket; aisSocket = null; s.terminate(); }
      ais.backoff = 2000; connectAIS();
    }
  } catch (e) {
    fb.lastError = e.message;
    console.error('[fallback] Poll failed:', e.message);
  } finally {
    fb.running = false;
  }
}

/* ── Boot ────────────────────────────────────────── */
loadState();

server.listen(PORT, () => {
  const fbOn = FALLBACK && FALLBACK_KEY && PROVIDERS[FALLBACK];
  console.log(`\n⚓ JME Fleet Operations Centre — http://localhost:${PORT}`);
  console.log(`   Vessels ${FLEET.length} | AIS key ${AIS_KEY ? '✓' : '✗'} | Fallback ${fbOn ? FALLBACK : 'off'} | State ${STATE_FILE}`);
  Object.values(state).filter(v => !v.mmsi).forEach(v =>
    console.warn(`   ⚠ ${v.name}: no MMSI — not on the AIS stream${v.imo ? ` (fallback can look it up by IMO ${v.imo})` : ''}`));
  if (FALLBACK && !PROVIDERS[FALLBACK]) console.warn(`   ⚠ Unknown FALLBACK_PROVIDER "${FALLBACK}"`);
  console.log('');
  connectAIS();
  if (fbOn) {
    setTimeout(pollFallback, 20_000);
    setInterval(pollFallback, Math.max(FALLBACK_POLL_MS, 5 * 60_000));
  }
});

function shutdown() {
  clearTimeout(saveTimer);
  saveNow();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

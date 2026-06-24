require('dotenv').config();
const express   = require('express');
const http      = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path      = require('path');

/* ── Config ─────────────────────────────────────── */
const PORT        = process.env.PORT        || 3000;
const AIS_KEY     = process.env.AIS_API_KEY || '';
const REFRESH_MS  = 15 * 60 * 1000;   // 15 minutes — broadcast "refresh" to all clients

/* ── Fleet definition ────────────────────────────── */
const VESSELS = [
  { mmsi:'538004721', name:'M/V MOTHER M',       type:'Handysize Bulk Carrier' },
  { mmsi:'538005001', name:'M/V MARIGOULA',       type:'Supramax Bulk Carrier'  },
  { mmsi:'538005436', name:'M/V ZOITSA SIGALA',   type:'Ultramax Bulk Carrier'  },
  { mmsi:'538008208', name:'M/V PRINCESS MARGO',  type:'Ultramax Bulk Carrier'  },
  { mmsi:'538011213', name:'M/V PRINCESS EIRINI', type:'Ultramax Bulk Carrier'  },
  { mmsi:'538000000', name:'M/V HARILAOS JUNIOR', type:'Ultramax Bulk Carrier'  }, // replace MMSI when confirmed
];

/* ── In-memory position cache ────────────────────── */
const cache = {};
VESSELS.forEach(v => {
  cache[v.mmsi] = { ...v, lat:null, lng:null, sog:null, cog:null, hdg:null, ns:null, dest:null, eta:null, ts:null };
});

/* ── Express + HTTP server ───────────────────────── */
const app    = express();
const server = http.createServer(app);

/* Flat repo — index.html is in the same directory as server.js */
const HTML_FILE  = path.join(__dirname, 'index.html');
const STATIC_DIR = __dirname;

app.use(express.json());
app.use(express.static(STATIC_DIR));

/* REST: full snapshot */
app.get('/api/vessels', (_req, res) => {
  res.json({ ok:true, ts: new Date().toISOString(), vessels: Object.values(cache) });
});

/* REST: health check (Railway will ping this) */
app.get('/api/health', (_req, res) => {
  const tracked = Object.values(cache).filter(v => v.ts).length;
  res.json({
    ok:         true,
    aisOnline:  aisSocket?.readyState === WebSocket.OPEN,
    tracked,
    total:      VESSELS.length,
    lastSeen:   Object.values(cache).map(v => v.ts).filter(Boolean).sort().pop() || null,
  });
});

/* REST: update operational data for a vessel (ETB / ETC / BOD / FW) */
app.patch('/api/vessels/:mmsi', (req, res) => {
  const v = cache[req.params.mmsi];
  if (!v) return res.status(404).json({ ok:false, error:'Vessel not found' });
  const { etb, etc, bod, fw } = req.body;
  if (etb !== undefined) v.etb = etb;
  if (etc !== undefined) v.etc = etc;
  if (bod !== undefined) v.bod = bod;
  if (fw  !== undefined) v.fw  = fw;
  broadcast({ type:'update', vessel:v });
  res.json({ ok:true, vessel:v });
});

/* Catch-all: serve index.html for every non-API GET (must come last) */
app.get('*', (_req, res) => {
  if (HTML_FILE) {
    res.sendFile(HTML_FILE);
  } else {
    res.status(404).send(
      '<h2>index.html not found</h2>' +
      '<p>Put <code>index.html</code> in a <code>public/</code> folder next to <code>server.js</code>.</p>'
    );
  }
});

/* ── Frontend WebSocket (/ws) ────────────────────── */
const wss     = new WebSocketServer({ server, path:'/ws' });
const clients = new Set();

wss.on('connection', ws => {
  clients.add(ws);
  // Send full snapshot immediately on connect
  safe(ws, JSON.stringify({ type:'snapshot', vessels: Object.values(cache) }));
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function safe(ws, data) {
  try { if (ws.readyState === WebSocket.OPEN) ws.send(data); } catch {}
}

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  clients.forEach(ws => safe(ws, msg));
}

/* ── AIS WebSocket (aisstream.io) ────────────────── */
let aisSocket     = null;
let aisRetryTimer = null;

function connectAIS() {
  if (!AIS_KEY) {
    console.log('[AIS] No AIS_API_KEY — demo positions only');
    return;
  }
  if (aisSocket && (aisSocket.readyState === WebSocket.OPEN ||
                    aisSocket.readyState === WebSocket.CONNECTING)) return;

  clearTimeout(aisRetryTimer);
  console.log('[AIS] Connecting to aisstream.io…');
  aisSocket = new WebSocket('wss://stream.aisstream.io/v0/stream');

  aisSocket.on('open', () => {
    console.log('[AIS] Connected ✓');
    aisSocket.send(JSON.stringify({
      APIKey:             AIS_KEY,
      BoundingBoxes:      [[[-90,-180],[90,180]]],
      FiltersShipMMSI:    VESSELS.map(v => v.mmsi),
      FilterMessageTypes: ['PositionReport','ShipStaticData','StandardClassBPositionReport'],
    }));
  });

  aisSocket.on('message', raw => {
    try { handleAIS(JSON.parse(raw)); } catch {}
  });

  aisSocket.on('close', (code, reason) => {
    console.log(`[AIS] Disconnected (${code}) — retry in 15 s`);
    aisRetryTimer = setTimeout(connectAIS, 15_000);
  });

  aisSocket.on('error', err => {
    console.error('[AIS] Error:', err.message);
  });
}

function handleAIS(msg) {
  const meta = msg.MetaData || {};
  const mmsi = meta.MMSI_String || String(meta.MMSI || '');
  const v    = cache[mmsi];
  if (!v) return;

  if (meta.latitude  != null) v.lat = parseFloat(meta.latitude.toFixed(5));
  if (meta.longitude != null) v.lng = parseFloat(meta.longitude.toFixed(5));
  v.ts = new Date().toISOString();

  const pos  = msg.Message?.PositionReport || msg.Message?.StandardClassBPositionReport || {};
  if (pos.Sog               != null) v.sog = pos.Sog;
  if (pos.Cog               != null) v.cog = pos.Cog;
  if (pos.TrueHeading       != null && pos.TrueHeading !== 511) v.hdg = pos.TrueHeading;
  if (pos.NavigationalStatus != null) v.ns  = pos.NavigationalStatus;

  const stat = msg.Message?.ShipStaticData || {};
  if (stat.Destination) v.dest = stat.Destination.trim();
  if (stat.Eta)         v.eta  = stat.Eta;

  broadcast({ type:'update', vessel:v });
}

/* ── 15-minute heartbeat ─────────────────────────── */
setInterval(() => {
  const tracked = Object.values(cache).filter(v => v.ts).length;
  console.log(`[15min] Tracked: ${tracked}/${VESSELS.length} | AIS: ${aisSocket?.readyState === WebSocket.OPEN ? 'ONLINE' : 'OFFLINE'}`);
  // Ensure AIS stays connected
  connectAIS();
  // Tell all clients to re-fetch (catches any missed updates)
  broadcast({ type:'refresh', ts: new Date().toISOString() });
}, REFRESH_MS);

/* ── Boot ────────────────────────────────────────── */
server.listen(PORT, () => {
  console.log(`\n⚓ JME Fleet Operations Centre`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   Vessels: ${VESSELS.length} | AIS key: ${AIS_KEY ? '✓ configured' : '✗ not set'}\n`);
  connectAIS();
});

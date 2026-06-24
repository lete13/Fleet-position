# JME Navigation S.A. — Fleet Operations Centre

Real-time bulk carrier fleet tracker. Built on Node.js + Express + WebSocket, designed for one-click Railway deployment.

## Architecture

```
Browser ──WebSocket(/ws)──► Express Server ──WebSocket──► aisstream.io (free AIS)
         ──GET /api/vessels (15-min poll)──►
```

- Server maintains a persistent WebSocket to **aisstream.io** and caches the latest position for each vessel.
- All connected browsers receive position updates in real-time via `/ws`.
- Every 15 minutes the server broadcasts a `refresh` event; browsers re-fetch `/api/vessels` as a safety net.
- Falls back to animated **demo mode** if the backend is unreachable (e.g. opening the HTML as a local file).

## Fleet

| # | Vessel | Type | MMSI |
|---|--------|------|------|
| 1 | M/V MOTHER M | Handysize | 538004721 |
| 2 | M/V MARIGOULA | Supramax | 538005001 |
| 3 | M/V ZOITSA SIGALA | Ultramax | 538005436 |
| 4 | M/V PRINCESS MARGO | Ultramax | 538008208 |
| 5 | M/V PRINCESS EIRINI | Ultramax | 538011213 |
| 6 | M/V HARILAOS JUNIOR | Ultramax | **538000000** ← update when confirmed (IMO 1041506) |

## Deploy to Railway

1. Push this repo to GitHub.
2. In Railway: **New Project → Deploy from GitHub repo** → select this repo.
3. Add environment variable:
   ```
   AIS_API_KEY=your_free_key_from_aisstream.io
   ```
4. Railway auto-detects Node.js, runs `npm start`, and assigns a public URL.
5. Health check: `GET /api/health`

## Local development

```bash
cp .env.example .env
# Edit .env and add your AIS_API_KEY
npm install
npm run dev     # uses nodemon for auto-restart
```

Open http://localhost:3000

## API

| Endpoint | Description |
|----------|-------------|
| `GET /api/vessels` | Full fleet snapshot (JSON) |
| `GET /api/health` | Server health + AIS connection status |
| `PATCH /api/vessels/:mmsi` | Update ETB / ETC / BOD / FW for a vessel |
| `WS /ws` | Real-time position stream |

## AIS Key

Register at [aisstream.io](https://aisstream.io) — free, no credit card. The key goes in `AIS_API_KEY`.

## Updating HARILAOS JUNIOR MMSI

When the vessel's MMSI is confirmed, edit line 13 of `server.js`:
```js
{ mmsi:'538000000',   // ← replace with actual 9-digit MMSI
```

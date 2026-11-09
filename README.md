# JME Navigation S.A. — Fleet Operations Centre

Fleet position board for the six bulk carriers. Node.js + Express + WebSocket, deployed on Railway.

## How positions get here

```
aisstream.io (shore AIS, stream) ─┐
second source (REST, optional) ───┼─► server ── last known fix per vessel ──► browsers (/ws + 60 s poll)
noon report typed in by hand ─────┘      │
                                         └─► data/state.json (survives restarts)
```

Every fix, whatever its source, goes through one function that keeps only the newest one per vessel, stamped with the time the position was **taken**, not the time the server heard about it.

The board never invents a position. Each card shows how old the fix is and where it came from:

| Fix age | Shown as |
|---|---|
| under 30 min | green — live |
| 30 min to 6 h | amber — last received time shown |
| over 6 h, or none | red — counted under **Stale Fix** in the header |

When a vessel was underway at its last fix and that fix is over 30 minutes old, the map also draws a **dashed outline**: the last fix carried forward on its last speed and course, for at most 12 hours. It is an estimate and is labelled as one.

## Why a vessel goes quiet

aisstream.io relays AIS picked up by shore stations (its documentation promises no satellite coverage and no uptime guarantee). A ship is only heard within VHF range of a station — tens of miles — so a bulk carrier on an ocean passage will show an ageing fix for days. That is a limit of the data source, not a fault in the app. Three ways to close the gap:

1. **Noon reports** — *Voyage Data → Position from noon report*. Accepts `12 34.5 S` / `045 12.3 E` or decimal degrees. Free, and correct mid-ocean.
2. **A second AIS network** — set `FALLBACK_PROVIDER` + `FALLBACK_API_KEY`. Adapters for VesselFinder and MyShipTracking are included (written against their published API docs; not yet run against a live key). Both are shore networks too: they fill coastal holes, not mid-ocean.
3. **Satellite AIS** — the only automatic mid-ocean fix. It is a paid product (Kpler/MarineTraffic, Spire, VesselFinder satellite plans). A new provider is one function in `PROVIDERS` in `server.js`.

## Fleet — `fleet.json`

| Vessel | Type | MMSI |
|---|---|---|
| M/V MOTHER M | Handysize | 538004721 |
| M/V MARIGOULA | Supramax | 538005001 |
| M/V ZOITSA SIGALA | Ultramax | 538005436 |
| M/V PRINCESS MARGO | Ultramax | 538008208 |
| M/V PRINCESS EIRINI | Ultramax | 538011213 |
| M/V HARILAOS JUNIOR | Ultramax | **not set** (IMO 1041506) |

HARILAOS JUNIOR has no MMSI on file, so it is not on the AIS stream. Either put the MMSI in `fleet.json`, or set `MMSI_OVERRIDES=harilaos-junior:<mmsi>` in Railway. With a second source configured it is looked up by IMO and the MMSI is learned automatically.

## Deploy on Railway

1. Deploy this repo; set `AIS_API_KEY` (free key from aisstream.io).
2. **Attach a Volume** to the service (any mount path, e.g. `/data`). Last known positions are stored there. Without it, each redeploy starts blank.
3. Optional: `EDIT_TOKEN`, `FALLBACK_*` — see `.env.example`.
4. Health and data-age check: `GET /api/health`.

## Local

```bash
cp .env.example .env    # add AIS_API_KEY
npm install
npm run dev             # http://localhost:3000
```

Opening `index.html` straight from disk shows a labelled demo with made-up positions.

## API

| Endpoint | |
|---|---|
| `GET /api/vessels` | Fleet snapshot with fix time, source, track and source status |
| `GET /api/health` | Fix age per vessel, AIS connection state, last error |
| `PATCH /api/vessels/:id` | `etb` `etc` `bod` `fw`, and/or a manual fix: `lat` `lng` `posTs` `sog` `cog` |
| `WS /ws` | `snapshot`, `update`, `status` messages |

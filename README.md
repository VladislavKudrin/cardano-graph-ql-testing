# graphql-performance

Real-time GraphQL load testing dashboard. Run multiple Locust instances against different endpoints simultaneously and compare metrics live.

## Setup

```bash
pip install -r requirements.txt
python server.py
```

Open http://localhost:5000.

## Usage

1. **Add instance** — click "+ Add Instance", give it a name and GraphQL endpoint URL (e.g. `http://your-server:3100`)
2. **Configure test** — set users, spawn rate, run time (empty = run forever), load profile
3. **Advanced** — optionally set a payment address, tx hash, and stake address to use real data in queries
4. **Start** — click Start; live charts update every 2 seconds
5. **Compare** — when 2+ instances are running, a comparison table appears at the bottom

## Load profiles

- **full** — all query types including address/tx/delegation lookups (requires real addresses)
- **light** — lightweight queries only (chain tip, blocks, epochs, assets, ADA supply)

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `5000` | Port for the dashboard server |

Per-instance Locust processes inherit env vars set via the UI (payment address, tx hash, stake address) or fall back to mainnet defaults in `locustfile.py`.

## Files

- `server.py` — FastAPI backend, manages Locust subprocess per instance
- `locustfile.py` — Locust scenario definitions
- `ui.html` — Single-page dashboard (served by server.py)

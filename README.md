# oTree Trading Bridge App

This folder is a standalone oTree project with one app (`trader_bridge_app`) that:

1. Waits for all players in a group.
2. Calls the external trading backend (`POST /trading/initiate`) with:
   - `num_human_traders = group size`
   - `num_days = NUM_ROUNDS`
   - per-day `dividends` array (from `data/dividends.csv`)
   - per-human `human_trader_params` (`initial_cash`, `initial_stocks`)
3. Stores one `trader_uuid` per `Player` in the oTree database.
4. Renders a websocket-based trading page using `js_vars`.

## Files

- `settings.py`, `requirements.txt`, `Procfile`: project-level oTree files.
- `trader_bridge_app/__init__.py`: oTree models/pages and backend initialization call.
- `trader_bridge_app/templates/trader_bridge_app/TradePage.html`: trading UI page.
- `_static/trader_bridge_app/vue/trader-embed.js`: built Vue app bundle loaded by `TradePage`.
- `_static/trader_bridge_app/vue/trader-embed.css`: built Vue app styles.
- `../london_trader_front/src/otree-main.js`: embedded Vue entrypoint used for oTree build.

## Add To oTree Settings

`settings.py` already includes this session config:

```python
SESSION_CONFIGS = [
    dict(
        name="trader_bridge_demo",
        display_name="Trader Bridge Demo",
        num_demo_participants=4,
        players_per_group=4,
        app_sequence=["trader_bridge_app"],
        trading_api_base="http://127.0.0.1:8001",
        trading_api_timeout_seconds=20,
        trading_day_duration=5,
        max_orders_per_minute=30,
        initial_midpoint=100,
        initial_spread=10,
        initial_cash=2600,
        initial_stocks=20,
        alert_streak_frequency=3,
        alert_window_size=5,
    ),
]
```

## Local Development

### Environment Variables

`settings.py` now auto-loads `otree_trader_bridge/.env` (if present) before reading config.

1. Create local env file:
```bash
cd otree_trader_bridge
cp .env.example .env
```

2. Update values in `.env` (example):
```bash
TRADING_API_BASE=https://london-trader-6b45bffdcd02.herokuapp.com
PLAYERS_PER_GROUP=2
OTREE_ADMIN_PASSWORD=replace-me
```

3. Sync `.env` values to Heroku config vars:
```bash
cd otree_trader_bridge
make heroku-config-set
```

Dry-run command preview:
```bash
make heroku-config-set-dry-run
```

Optional: override app explicitly:
```bash
make heroku-config-set APP=<your-heroku-app>
```

Run both services in parallel:

1. Build the Vue frontend for oTree (from repo root):
```bash
cd london_trader_front
npm run build:otree
```

2. Trading backend:
```bash
cd ../trading_platform_app
uvicorn client_connector.main:app --reload --port 8001
```

3. oTree project:
```bash
cd otree_trader_bridge
otree devserver
```

Then open the oTree session URL in multiple browser tabs/players. Each participant receives a unique trader UUID at the wait page barrier.

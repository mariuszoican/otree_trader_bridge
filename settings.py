from pathlib import Path
from os import environ


def _load_local_env():
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        environ.setdefault(key, value)


_load_local_env()

PLAYERS_PER_GROUP = max(2, int(environ.get("PLAYERS_PER_GROUP", 2)))


SESSION_CONFIGS = [
    dict(
        name="intro_only",
        display_name="Intro Only",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        app_sequence=["intro"],
    ),
    dict(
        name="trader_bridge_demo",
        display_name="Trader Bridge Demo",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        app_sequence=["trader_bridge_app"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=30,
        hybrid_noise_traders=1,
        treatments=["gh", "gh", "nh", "nh", "gm", "nm"],
        initial_midpoint=100,
        initial_spread=10,
        initial_cash=2600,
        initial_stocks=20,
        alert_streak_frequency=3,
        alert_window_size=5,
    ),
    dict(
        name="post_exp_only",
        display_name="Post-Experiment Only",
        num_demo_participants=2,
        players_per_group=PLAYERS_PER_GROUP,
        app_sequence=["post_exp"],
    ),
    dict(
        name="full_study",
        display_name="Full Study (Intro + Market + Post)",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        app_sequence=["intro", "trader_bridge_app", "post_exp"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=30,
        hybrid_noise_traders=1,
        treatments=["gh", "gh", "nh", "nh", "gm", "nm"],
        initial_midpoint=100,
        initial_spread=10,
        initial_cash=2600,
        initial_stocks=20,
        alert_streak_frequency=3,
        alert_window_size=5,
    ),
]


SESSION_CONFIG_DEFAULTS = dict(
    real_world_currency_per_point=1.00,
    participation_fee=0.00,
    forecast_bonus_threshold_pct=1,
    dividend_values=[0, 4, 8, 20],
    doc="",
)

PARTICIPANT_FIELDS = []
SESSION_FIELDS = []

LANGUAGE_CODE = "en"
REAL_WORLD_CURRENCY_CODE = "USD"
USE_POINTS = True

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = environ.get("OTREE_ADMIN_PASSWORD")

DEMO_PAGE_INTRO_HTML = ""
SECRET_KEY = "replace-me-in-production"

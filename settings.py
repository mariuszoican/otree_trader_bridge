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

_BASE_PLAYERS_PER_GROUP = max(2, int(environ.get("PLAYERS_PER_GROUP", 2)))
_BASE_TREATMENTS = ["gh", "gh", "nh", "nh", "gm", "nm"]
_ALL_HYBRID_TREATMENTS = ["gm", "gm", "nm", "nm", "gm", "nm"]

# TEMP TEST MODE: force singleton groups and guarantee NT presence in every market.
# Remove this block after timing/debugging is finished.
TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE = False
# TEMP TEST MODE: keep all markets hybrid while preserving normal group sizes.
# Remove this block after timing/debugging is finished.
TEMP_ALL_HYBRID_MARKETS = False

PLAYERS_PER_GROUP = None if TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE else _BASE_PLAYERS_PER_GROUP
SESSION_TREATMENTS = (
    _ALL_HYBRID_TREATMENTS
    if TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE
    else (_ALL_HYBRID_TREATMENTS if TEMP_ALL_HYBRID_MARKETS else list(_BASE_TREATMENTS))
)
HYBRID_NOISE_TRADER_PROBABILITY = 1 if TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE else 0.2
TEMP_SINGLETON_GROUPS = bool(TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE)

# App defaults still read PLAYERS_PER_GROUP from env for display/default logic.
environ["PLAYERS_PER_GROUP"] = "1" if TEMP_SINGLE_PLAYER_ALL_NT_TEST_MODE else str(_BASE_PLAYERS_PER_GROUP)

ROOMS = [
    dict(
        name="trader_bridge_room",
        display_name="Trader Bridge Room",
 
    )
]
SESSION_CONFIGS = [
    dict(
        name="intro_only",
        display_name="Intro Only",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_intro_video_page=True,
        app_sequence=["intro"],
    ),
    dict(
        name="main",
        use_browser_bots=False,
        display_name="Trader Bridge Demo",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        app_sequence=["trader_bridge_app"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=30,
        noise_trader_start_second=5,
        hybrid_noise_traders=1,
        hybrid_noise_trader_probability=HYBRID_NOISE_TRADER_PROBABILITY,
        treatments=SESSION_TREATMENTS,
        initial_midpoint=120,
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
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_lab_contact_page=True,
        app_sequence=["post_exp"],
    ),
    dict(
        name="full_study",
        display_name="Full Study (Intro + Market + Post)",
        num_demo_participants=12,
        players_per_group=PLAYERS_PER_GROUP,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_intro_video_page=True,
        show_lab_contact_page=True,
        app_sequence=["intro", "trader_bridge_app", "post_exp"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=30,
        noise_trader_start_second=5,
        hybrid_noise_traders=1,
        hybrid_noise_trader_probability=HYBRID_NOISE_TRADER_PROBABILITY,
        treatments=SESSION_TREATMENTS,
        initial_midpoint=120,
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
    fee_per_correct_answer=10,
    hybrid_noise_trader_probability=0.2,
    trading_day_duration=1,
    forecast_bonus_amount=20,
    forecast_bonus_threshold_pct=5,
    dividend_values=[0, 4, 8, 20],
    show_intro_video_page=True,
    show_lab_contact_page=True,
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

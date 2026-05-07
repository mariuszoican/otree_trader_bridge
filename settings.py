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

_BASE_PREFERRED_PLAYERS_PER_GROUP = max(2, int(environ.get("PLAYERS_PER_GROUP", 2)))
# All four treatments are human-only; they vary along the gamification
# dimension. The cycle gives 2 ghp + 2 ng + 1 gh + 1 gp every 6 groups.
_BASE_TREATMENTS = ["ghp", "ghp", "ng", "ng", "gh", "gp"]

# Test mode: every player is alone in their own group with a filler noise
# trader, so a single tab can drive the full session end-to-end. Useful for
# quick smoke tests; do not enable for live sessions. Driven by env var so
# you can flip it without editing settings.
TEMP_SINGLE_PLAYER_TEST_MODE = (
    str(environ.get("TEMP_SINGLE_PLAYER_TEST_MODE", "")).strip().lower()
    in {"1", "true", "yes", "on"}
)

PREFERRED_PLAYERS_PER_GROUP = 1 if TEMP_SINGLE_PLAYER_TEST_MODE else _BASE_PREFERRED_PLAYERS_PER_GROUP
PLAYERS_PER_GROUP = None
SESSION_TREATMENTS = list(_BASE_TREATMENTS)
TEMP_SINGLETON_GROUPS = TEMP_SINGLE_PLAYER_TEST_MODE

# App defaults still read PLAYERS_PER_GROUP from env for display/default logic.
environ["PLAYERS_PER_GROUP"] = str(PREFERRED_PLAYERS_PER_GROUP)

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
        players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        preferred_players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        soft_group_matching_enabled=False,
        small_remainder_force_nt_below_size=6,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_intro_video_page=True,
        app_sequence=["intro"],
    ),
    dict(
        name="main",
        use_browser_bots=False,
        display_name="Trader Bridge Demo",
        num_demo_participants=12,
        players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        preferred_players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        soft_group_matching_enabled=False,
        small_remainder_force_nt_below_size=6,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        app_sequence=["trader_bridge_app"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=10,
        noise_trader_start_second=5,
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
        players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        preferred_players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        soft_group_matching_enabled=False,
        small_remainder_force_nt_below_size=6,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_lab_contact_page=True,
        app_sequence=["post_exp"],
    ),
    dict(
        name="full_study",
        display_name="Full Study (Intro + Market + Post)",
        num_demo_participants=12,
        players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        preferred_players_per_group=PREFERRED_PLAYERS_PER_GROUP,
        soft_group_matching_enabled=True,
        small_remainder_force_nt_below_size=4,
        temporary_singleton_groups=TEMP_SINGLETON_GROUPS,
        show_intro_video_page=True,
        show_lab_contact_page=True,
        app_sequence=["intro", "trader_bridge_app", "post_exp"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8001"),
        trading_api_timeout_seconds=20,
        trading_day_duration=1,
        step=1,
        max_orders_per_minute=10,
        noise_trader_start_second=5,
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
    real_world_currency_per_point=0.002,
    participation_fee=0.00,
    fee_per_correct_answer=10,
    trading_day_duration=1,
    forecast_bonus_amount=20,
    forecast_bonus_threshold_pct=5,
    preferred_players_per_group=PREFERRED_PLAYERS_PER_GROUP,
    soft_group_matching_enabled=False,
    # If a tail group ends up smaller than this many humans, the trading
    # backend is started with a single noise trader so the market remains
    # viable. This is purely about filling out incomplete groups; it does
    # not change the treatment assigned to those participants.
    small_remainder_force_nt_below_size=6,
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

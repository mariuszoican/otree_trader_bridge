from os import environ


SESSION_CONFIGS = [
    dict(
        name="trader_bridge_demo",
        display_name="Trader Bridge Demo",
        num_demo_participants=4,
        players_per_group=4,
        app_sequence=["trader_bridge_app"],
        trading_api_base=environ.get("TRADING_API_BASE", "http://localhost:8000"),
        trading_api_timeout_seconds=20,
        trading_day_duration=5,
        step=1,
        max_orders_per_minute=30,
        initial_midpoint=100,
        initial_spread=10,
        initial_cash=2600,
        initial_stocks=20,
        alert_streak_frequency=3,
        alert_window_size=5,
        allow_self_trade=True,
        gamified=True,
    ),
]


SESSION_CONFIG_DEFAULTS = dict(
    real_world_currency_per_point=1.00,
    participation_fee=0.00,
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

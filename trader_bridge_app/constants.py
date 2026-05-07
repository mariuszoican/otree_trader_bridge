import csv
import os
from pathlib import Path

from otree.api import BaseConstants

from .utils import _as_float


def _load_dividends_csv_for_constants():
    path = Path(__file__).resolve().parent.parent / "data" / "dividends.csv"
    if not path.exists():
        raise RuntimeError(f"Missing dividends CSV at {path}")
    values = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return values
    start_idx = 0
    first = [c.strip().lower() for c in rows[0]]
    if first and first[0] in {"dividend_per_share", "dividend", "value"}:
        start_idx = 1
    for row in rows[start_idx:]:
        if not row:
            continue
        text = str(row[0]).strip()
        if not text:
            continue
        values.append(_as_float(text, 0.0))
    return values


def _market_days_schedule():
    training_days = max(1, int(os.getenv("TRAINING_DAYS", 3)))
    days_per_market = max(1, int(os.getenv("DAYS_PER_MARKET", 15)))
    num_main_markets = max(1, int(os.getenv("NUM_MAIN_MARKETS", 2)))
    return (training_days,) + (days_per_market,) * num_main_markets


class C(BaseConstants):
    """
    Constants class for the trader bridge application.

    The session always starts with a single training market (3 periods by
    default) followed by the main trading markets (two 15-period markets by
    default). Market 1 is the training market and is excluded from payable
    market selection.

    Attributes:
        NAME_IN_URL (str): URL identifier for the app.
        PLAYERS_PER_GROUP (None): No group structure enforced.
        MARKET_DAYS (tuple): Per-market day counts. Index 0 is the training
            market; subsequent entries are the main markets.
        NUM_MARKETS (int): Total markets, including training.
        TRAINING_MARKET_NUMBER (int): 1-based index of the training market.
        DAYS_PER_MARKET (int): Days in each main (non-training) market.
        TRAINING_DAYS (int): Days in the training market.
        NUM_ROUNDS (int): Total rounds across all markets.

        DEFAULT_TRADING_API_BASE (str): Base URL for the trading API server.
        DEFAULT_API_TIMEOUT_SECONDS (int): Timeout duration for API requests in seconds.
        DEFAULT_TRADING_DAY_DURATION (int): Duration of a trading day in simulation time units.
        DEFAULT_STEP (int): Single step increment for simulation progression.
        DEFAULT_MAX_ORDERS_PER_MINUTE (int): Maximum number of orders a player can submit per minute.
        DEFAULT_INITIAL_MIDPOINT (int): Initial midpoint price for traded assets.
        DEFAULT_INITIAL_SPREAD (int): Initial bid-ask spread for traded assets.
        DEFAULT_INITIAL_CASH (int): Initial cash allocation per player.
        DEFAULT_INITIAL_STOCKS (int): Initial stock allocation per player.
        DEFAULT_ALERT_STREAK_FREQUENCY (int): Frequency threshold for alert notifications.
        DEFAULT_ALERT_WINDOW_SIZE (int): Window size for calculating alert metrics.
        DEFAULT_GROUP_SIZE (int): Number of players per trading group.

        TREATMENTS (tuple): Available treatment conditions. All four are
            human-only; they vary along the gamification dimension:
                - ``ghp``: gamified with hedonic + price (full gamification)
                - ``ng``:  non-gamified (control)
                - ``gh``:  gamified hedonic only (badges/achievements)
                - ``gp``:  gamified price only (price-trend notifications)
        TREATMENT_MARKET_DESIGN (dict): Treatment -> gamification flavor label.
        TREATMENT_GROUP_COMPOSITION (dict): Treatment -> group composition.
            With the redesign, every treatment is ``human_only``.
        TREATMENT_FLAGS (dict): Treatment -> ``(hedonic_enabled, info_enabled)``
            tuple, used to drive the front-end UI.
    """

    NAME_IN_URL = "trader_bridge"
    PLAYERS_PER_GROUP = None

    MARKET_DAYS = _market_days_schedule()
    NUM_MARKETS = len(MARKET_DAYS)
    TRAINING_MARKET_NUMBER = 1
    TRAINING_DAYS = MARKET_DAYS[TRAINING_MARKET_NUMBER - 1]
    # Days per (main, non-training) market. All non-training markets share the
    # same length in the default schedule.
    DAYS_PER_MARKET = MARKET_DAYS[1] if NUM_MARKETS > 1 else MARKET_DAYS[0]
    NUM_ROUNDS = sum(MARKET_DAYS)

    DEFAULT_TRADING_API_BASE = "http://127.0.0.1:8001"
    DEFAULT_API_TIMEOUT_SECONDS = 20
    DEFAULT_TRADING_DAY_DURATION = 2
    DEFAULT_STEP = 1
    DEFAULT_MAX_ORDERS_PER_MINUTE = 30
    DEFAULT_NOISE_TRADER_START_SECOND = 5
    DEFAULT_INITIAL_MIDPOINT = 100
    DEFAULT_INITIAL_SPREAD = 10
    DEFAULT_INITIAL_CASH = 2600
    DEFAULT_INITIAL_STOCKS = 20
    DEFAULT_ALERT_STREAK_FREQUENCY = 3
    DEFAULT_ALERT_WINDOW_SIZE = 5
    DEFAULT_GROUP_SIZE = max(2, int(os.getenv("PLAYERS_PER_GROUP", 2)))

    DEFAULT_FORECAST_BONUS_AMOUNT = 10
    DEFAULT_FORECAST_BONUS_THRESHOLD_PCT = 5
    DEFAULT_DIVIDEND_VALUES = (0, 4, 8, 20)
    TREATMENTS = ("ghp", "ng", "gh", "gp")
    TREATMENT_MARKET_DESIGN = {
        "ghp": "gamified",
        "ng":  "non_gamified",
        "gh":  "hedonic_only",
        "gp":  "info_only",
    }
    TREATMENT_GROUP_COMPOSITION = {
        "ghp": "human_only",
        "ng":  "human_only",
        "gh":  "human_only",
        "gp":  "human_only",
    }
    # (hedonic_enabled, info_enabled): hedonic = badges/achievements/confetti;
    # info = price-trend notifications.
    TREATMENT_FLAGS = {
        "ghp": (True, True),
        "ng":  (False, False),
        "gh":  (True, False),
        "gp":  (False, True),
    }
    DEFAULT_HUMAN_TRADER_ENDOWMENTS = (
        (2600.0, 20),
        (3800.0, 10),
    )
    DIVIDEND_SCHEDULE = tuple(_load_dividends_csv_for_constants())

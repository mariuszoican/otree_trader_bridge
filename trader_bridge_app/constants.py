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


class C(BaseConstants):
    """
    Constants class for the trader bridge application.

    Attributes:
        NAME_IN_URL (str): URL identifier for the app.
        PLAYERS_PER_GROUP (None): No group structure enforced.
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
        DEFAULT_HYBRID_NOISE_TRADERS (int): Number of noise trader agents in hybrid treatment groups.

        TREATMENTS (tuple): Available treatment conditions.
        TREATMENT_MARKET_DESIGN (dict): Maps treatments to market design types (gamified or non-gamified).
        TREATMENT_GROUP_COMPOSITION (dict): Maps treatments to group composition types (human_only or hybrid).
    """

    NAME_IN_URL = "trader_bridge"
    PLAYERS_PER_GROUP = max(2, int(os.getenv("PLAYERS_PER_GROUP", 2)))
    NUM_MARKETS = max(1, int(os.getenv("NUM_MARKETS", 2)))
    DAYS_PER_MARKET = max(1, int(os.getenv("DAYS_PER_MARKET", 2)))
    NUM_ROUNDS = int(os.getenv("NUM_ROUNDS", NUM_MARKETS * DAYS_PER_MARKET))

    DEFAULT_TRADING_API_BASE = "http://127.0.0.1:8001"
    DEFAULT_API_TIMEOUT_SECONDS = 20
    DEFAULT_TRADING_DAY_DURATION = 2
    DEFAULT_STEP = 1
    DEFAULT_MAX_ORDERS_PER_MINUTE = 30
    DEFAULT_INITIAL_MIDPOINT = 100
    DEFAULT_INITIAL_SPREAD = 10
    DEFAULT_INITIAL_CASH = 2600
    DEFAULT_INITIAL_STOCKS = 20
    DEFAULT_ALERT_STREAK_FREQUENCY = 3
    DEFAULT_ALERT_WINDOW_SIZE = 5
    DEFAULT_GROUP_SIZE = PLAYERS_PER_GROUP
    DEFAULT_HYBRID_NOISE_TRADERS = 1
    DEFAULT_FORECAST_BONUS_AMOUNT = 1
    DEFAULT_FORECAST_BONUS_THRESHOLD_PCT = 1
    DEFAULT_DIVIDEND_VALUES = (0, 4, 8, 20)
    TREATMENTS = ("gh", "nh", "gm", "nm")
    TREATMENT_MARKET_DESIGN = {
        "gh": "gamified",
        "gm": "gamified",
        "nh": "non_gamified",
        "nm": "non_gamified",
    }
    TREATMENT_GROUP_COMPOSITION = {
        "gh": "human_only",
        "nh": "human_only",
        "gm": "hybrid",
        "nm": "hybrid",
    }
    DEFAULT_NUM_DAYS = DAYS_PER_MARKET
    DEFAULT_HUMAN_TRADER_ENDOWMENTS = (
        (2600.0, 20),
        (3800.0, 10),
    )
    DIVIDEND_SCHEDULE = tuple(_load_dividends_csv_for_constants())

import json
import os
import socket
import sqlite3
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, parse, request

from otree.api import *

doc = """
oTree app that initializes one external trader UUID per group participant and
opens a websocket-driven trading page.
"""


class C(BaseConstants):
    NAME_IN_URL = "trader_bridge"
    PLAYERS_PER_GROUP = None
    NUM_ROUNDS = 1

    DEFAULT_TRADING_API_BASE = "http://127.0.0.1:8001"
    DEFAULT_API_TIMEOUT_SECONDS = 20
    DEFAULT_TRADING_DAY_DURATION = 5
    DEFAULT_STEP = 1
    DEFAULT_MAX_ORDERS_PER_MINUTE = 30
    DEFAULT_INITIAL_MIDPOINT = 100
    DEFAULT_INITIAL_SPREAD = 10
    DEFAULT_INITIAL_CASH = 2600
    DEFAULT_INITIAL_STOCKS = 20
    DEFAULT_ALERT_STREAK_FREQUENCY = 3
    DEFAULT_ALERT_WINDOW_SIZE = 5
    DEFAULT_ALLOW_SELF_TRADE = True


class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    trading_session_uuid = models.StringField(blank=True)
    trading_api_base = models.StringField(blank=True)
    trading_ws_base = models.StringField(blank=True)
    trading_init_error = models.LongStringField(blank=True)
    trading_day_duration_minutes = models.IntegerField(initial=C.DEFAULT_TRADING_DAY_DURATION)


class Player(BasePlayer):
    trader_uuid = models.StringField(blank=True)


def _log(message, **context):
    ts = datetime.now(timezone.utc).isoformat()
    if context:
        details = ", ".join(f"{k}={repr(v)}" for k, v in sorted(context.items()))
        print(f"[trader_bridge][{ts}] {message} | {details}", flush=True)
    else:
        print(f"[trader_bridge][{ts}] {message}", flush=True)


def creating_session(subsession: Subsession):
    _log(
        "creating_session start",
        round_number=subsession.round_number,
        subsession_id=getattr(subsession, "id", None),
        session_code=getattr(subsession.session, "code", None),
    )
    if subsession.round_number != 1:
        _log("creating_session skipped because round_number != 1", round_number=subsession.round_number)
        return

    players = subsession.get_players()
    if not players:
        _log("creating_session found no players")
        return

    _log("creating_session players loaded", player_ids=[p.id_in_subsession for p in players], num_players=len(players))

    desired_size = _as_int(subsession.session.config.get("players_per_group", 4), 4)
    desired_size = max(2, desired_size)
    _log("creating_session using group size", requested=subsession.session.config.get("players_per_group"), applied=desired_size)

    matrix = []
    for idx in range(0, len(players), desired_size):
        matrix.append(players[idx: idx + desired_size])
    subsession.set_group_matrix(matrix)
    _log(
        "creating_session group matrix set",
        group_sizes=[len(g) for g in matrix],
        groups=[[p.id_in_subsession for p in g] for g in matrix],
    )


def _as_int(value, fallback):
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _as_bool(value, fallback=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return fallback


def _normalize_http_base(base_url):
    url = str(base_url or "").strip()
    if not url:
        return C.DEFAULT_TRADING_API_BASE
    return url.rstrip("/")


def _ws_base_from_http(http_base):
    pieces = parse.urlsplit(http_base)
    scheme = "wss" if pieces.scheme == "https" else "ws"
    path = pieces.path.rstrip("/")
    return parse.urlunsplit((scheme, pieces.netloc, path, "", ""))


def _post_json(url, payload, timeout_seconds):
    _log("HTTP POST starting", url=url, timeout_seconds=timeout_seconds, payload=payload)
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            charset = resp.headers.get_content_charset("utf-8")
            raw = resp.read().decode(charset)
            _log(
                "HTTP POST response received",
                status=getattr(resp, "status", None),
                reason=getattr(resp, "reason", None),
                headers=dict(resp.headers.items()),
                raw_body=raw,
            )
            parsed = json.loads(raw)
            _log("HTTP POST response parsed", parsed=parsed)
            return parsed
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        _log("HTTP POST failed with HTTPError", status=exc.code, reason=exc.reason, body=detail)
        raise RuntimeError(f"Trading API HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        _log("HTTP POST failed with URLError", reason=str(exc.reason), type=str(type(exc.reason)))
        raise RuntimeError(f"Trading API unreachable at {url}: {exc.reason}") from exc
    except socket.timeout as exc:
        _log("HTTP POST failed with socket.timeout", timeout_seconds=timeout_seconds)
        raise RuntimeError(f"Trading API timed out after {timeout_seconds} seconds.") from exc
    except Exception as exc:
        _log("HTTP POST failed with unexpected error", error=str(exc), traceback=traceback.format_exc())
        raise


def _build_initiate_payload(group: Group, num_players: int):
    cfg = group.session.config
    return dict(
        num_human_traders=num_players,
        trading_day_duration=_as_int(
            cfg.get("trading_day_duration", C.DEFAULT_TRADING_DAY_DURATION),
            C.DEFAULT_TRADING_DAY_DURATION,
        ),
        step=_as_int(
            cfg.get("step", C.DEFAULT_STEP),
            C.DEFAULT_STEP,
        ),
        max_orders_per_minute=_as_int(
            cfg.get("max_orders_per_minute", C.DEFAULT_MAX_ORDERS_PER_MINUTE),
            C.DEFAULT_MAX_ORDERS_PER_MINUTE,
        ),
        initial_midpoint=float(cfg.get("initial_midpoint", C.DEFAULT_INITIAL_MIDPOINT)),
        initial_spread=float(cfg.get("initial_spread", C.DEFAULT_INITIAL_SPREAD)),
        initial_cash=float(cfg.get("initial_cash", C.DEFAULT_INITIAL_CASH)),
        initial_stocks=_as_int(cfg.get("initial_stocks", C.DEFAULT_INITIAL_STOCKS), C.DEFAULT_INITIAL_STOCKS),
        alert_streak_frequency=_as_int(
            cfg.get("alert_streak_frequency", C.DEFAULT_ALERT_STREAK_FREQUENCY),
            C.DEFAULT_ALERT_STREAK_FREQUENCY,
        ),
        alert_window_size=_as_int(cfg.get("alert_window_size", C.DEFAULT_ALERT_WINDOW_SIZE), C.DEFAULT_ALERT_WINDOW_SIZE),
        allow_self_trade=_as_bool(cfg.get("allow_self_trade", C.DEFAULT_ALLOW_SELF_TRADE), C.DEFAULT_ALLOW_SELF_TRADE),
    )


def after_all_players_arrive(group: Group):
    _log(
        "after_all_players_arrive start",
        group_id=getattr(group, "id", None),
        subsession_id=getattr(group.subsession, "id", None),
        session_code=getattr(group.session, "code", None),
        round_number=getattr(group.subsession, "round_number", None),
    )
    players = sorted(group.get_players(), key=lambda p: p.id_in_group)
    num_players = len(players)
    cfg = group.session.config
    _log(
        "after_all_players_arrive loaded players and config",
        num_players=num_players,
        player_ids=[p.id_in_group for p in players],
        session_config=dict(cfg),
    )

    http_base = _normalize_http_base(
        cfg.get("trading_api_base", os.getenv("TRADING_API_BASE", C.DEFAULT_TRADING_API_BASE))
    )
    timeout_seconds = _as_int(
        cfg.get("trading_api_timeout_seconds", C.DEFAULT_API_TIMEOUT_SECONDS),
        C.DEFAULT_API_TIMEOUT_SECONDS,
    )

    group.trading_api_base = http_base
    group.trading_ws_base = _ws_base_from_http(http_base)
    group.trading_day_duration_minutes = _as_int(
        cfg.get("trading_day_duration", C.DEFAULT_TRADING_DAY_DURATION),
        C.DEFAULT_TRADING_DAY_DURATION,
    )
    _log(
        "after_all_players_arrive resolved endpoints",
        http_base=http_base,
        ws_base=group.trading_ws_base,
        timeout_seconds=timeout_seconds,
        trading_day_duration_minutes=group.trading_day_duration_minutes,
    )

    try:
        payload = _build_initiate_payload(group, num_players)
        _log("after_all_players_arrive built payload", payload=payload)
        response = _post_json(f"{http_base}/trading/initiate", payload, timeout_seconds)
        _log("after_all_players_arrive received response", response=response)
        data = response.get("data") or {}
        _log("after_all_players_arrive extracted data section", data=data, data_keys=list(data.keys()))

        trading_session_uuid = data.get("trading_session_uuid")
        human_traders = data.get("human_traders") or []
        _log(
            "after_all_players_arrive parsed trading identifiers",
            trading_session_uuid=trading_session_uuid,
            human_traders=human_traders,
            human_traders_count=len(human_traders),
        )
        if not trading_session_uuid:
            _log("after_all_players_arrive validation failed: missing trading_session_uuid")
            raise RuntimeError("Trading API response missing data.trading_session_uuid.")
        if len(human_traders) < num_players:
            _log(
                "after_all_players_arrive validation failed: not enough human traders",
                expected=num_players,
                received=len(human_traders),
            )
            raise RuntimeError(
                f"Trading API returned {len(human_traders)} human trader UUIDs for {num_players} players."
            )

        group.trading_session_uuid = str(trading_session_uuid)
        group.trading_init_error = ""
        _log("after_all_players_arrive storing success state", trading_session_uuid=group.trading_session_uuid)

        for player, trader_uuid in zip(players, human_traders):
            trader_id = str(trader_uuid)
            player.trader_uuid = trader_id
            player.participant.vars["trader_uuid"] = trader_id
            _log(
                "after_all_players_arrive assigned trader UUID",
                player_id_in_group=player.id_in_group,
                player_id_in_subsession=player.id_in_subsession,
                trader_uuid=trader_id,
            )
        _log("after_all_players_arrive completed successfully")
    except Exception as exc:
        group.trading_session_uuid = ""
        group.trading_init_error = str(exc)
        _log(
            "after_all_players_arrive failed",
            error=str(exc),
            error_type=str(type(exc)),
            traceback=traceback.format_exc(),
        )
        for player in players:
            player.trader_uuid = ""
            _log(
                "after_all_players_arrive cleared player trader_uuid due to error",
                player_id_in_group=player.id_in_group,
                player_id_in_subsession=player.id_in_subsession,
            )


class Intro(Page):
    pass


class SyncTradingSession(WaitPage):
    title_text = "Preparing Trading Session"
    body_text = "Please wait while the group trading session is created."
    after_all_players_arrive = after_all_players_arrive


class InitFailed(Page):
    @staticmethod
    def is_displayed(player: Player):
        return bool(player.group.trading_init_error)

    @staticmethod
    def vars_for_template(player: Player):
        return dict(
            error_message=player.group.trading_init_error,
            trading_api_base=player.group.trading_api_base,
        )


class TradePage(Page):
    use_standard_layout = False

    @staticmethod
    def is_displayed(player: Player):
        return not player.group.trading_init_error and bool(player.trader_uuid)

    @staticmethod
    def vars_for_template(player: Player):
        ws_url = f"{player.group.trading_ws_base}/trader/{player.trader_uuid}"
        return dict(
            ws_url=ws_url,
            trader_uuid=player.trader_uuid,
            trading_api_base=player.group.trading_api_base,
            trading_session_uuid=player.group.trading_session_uuid,
        )

    @staticmethod
    def js_vars(player: Player):
        ws_url = f"{player.group.trading_ws_base}/trader/{player.trader_uuid}"
        gamified = _as_bool(player.session.config.get("gamified", True), True)
        return dict(
            wsUrl=ws_url,
            wsBase=player.group.trading_ws_base,
            traderUuid=player.trader_uuid,
            httpUrl=f"{player.group.trading_api_base}/",
            tradingApiBase=player.group.trading_api_base,
            tradingSessionUuid=player.group.trading_session_uuid,
            playerIdInGroup=player.id_in_group,
            gamified=gamified,
        )

    @staticmethod
    def get_timeout_seconds(player: Player):
        duration_minutes = player.group.trading_day_duration_minutes or C.DEFAULT_TRADING_DAY_DURATION
        return max(15, int(duration_minutes * 60) + 30)


class Results(Page):
    pass


page_sequence = [
    # Intro,
      SyncTradingSession, 
      InitFailed,
        TradePage, 
        Results]


def _resolve_sqlite_path_for_exports():
    db_url = str(os.getenv("DATABASE_URL", "")).strip()
    if db_url.startswith("sqlite:///"):
        return db_url[len("sqlite:///") :]
    # Local default for this oTree project.
    return str((Path(__file__).resolve().parents[1] / "db.sqlite3").resolve())


def _resolve_export_backend_and_target():
    db_url = str(os.getenv("DATABASE_URL", "")).strip()
    if db_url:
        if db_url.startswith("postgresql+psycopg2://"):
            # psycopg2 expects postgresql:// URI.
            return "postgres", db_url.replace("postgresql+psycopg2://", "postgresql://", 1)
        if db_url.startswith("postgres://"):
            return "postgres", "postgresql://" + db_url[len("postgres://") :]
        if db_url.startswith("postgresql://"):
            return "postgres", db_url
        if db_url.startswith("sqlite:///"):
            return "sqlite", db_url[len("sqlite:///") :]
    return "sqlite", str((Path(__file__).resolve().parents[1] / "db.sqlite3").resolve())


def _fetch_export_rows(sql_query, export_name, missing_table_hint):
    backend, target = _resolve_export_backend_and_target()

    if backend == "sqlite":
        if not target or not os.path.exists(target):
            _log(f"{export_name} skipped: sqlite path missing", sqlite_path=target)
            return []
        try:
            conn = sqlite3.connect(target)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql_query)
            return cur.fetchall()
        except sqlite3.OperationalError as exc:
            _log(f"{export_name} skipped: {missing_table_hint}", error=str(exc))
            return []
        except Exception as exc:
            _log(f"{export_name} failed", error=str(exc), traceback=traceback.format_exc())
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except Exception as exc:
        _log(f"{export_name} skipped: psycopg2 unavailable", error=str(exc), db_target=target)
        return []

    try:
        conn = psycopg2.connect(target)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql_query)
                return cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        exc_text = str(exc).lower()
        if "relation" in exc_text and "does not exist" in exc_text:
            _log(f"{export_name} skipped: {missing_table_hint}", error=str(exc))
        else:
            _log(f"{export_name} failed", error=str(exc), traceback=traceback.format_exc())
        return []


def _extract_quantity_from_transaction_json(transaction_json_raw):
    try:
        parsed = json.loads(str(transaction_json_raw or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""

    if not isinstance(parsed, dict):
        return ""

    for key in ("quantity", "amount", "fill_amount", "transaction_amount"):
        value = parsed.get(key)
        if value is not None:
            return value
    return ""


def custom_export_messages(players):
    yield [
        "trading_session_uuid",
        "recipient_trader_uuid",
        "recipient_trader_type",
        "message_type",
        "content_json",
        "timestamp",
        "created_ts",
    ]

    rows = _fetch_export_rows(
        """
        SELECT m.trading_session_uuid,
               m.recipient_trader_uuid,
               t.trader_type AS recipient_trader_type,
               m.message_type,
               m.content_json,
               m.timestamp,
               m.created_ts
        FROM trading_platform_messages AS m
        LEFT JOIN trading_platform_traders AS t
          ON t.trading_session_uuid = m.trading_session_uuid
         AND t.trader_uuid = m.recipient_trader_uuid
        ORDER BY m.id ASC
        """,
        export_name="custom_export_messages",
        missing_table_hint="trading_platform_messages unavailable",
    )

    for row in rows:
        trading_session_uuid = str(row["trading_session_uuid"] or "")
        recipient_uuid = str(row["recipient_trader_uuid"] or "")
        recipient_type = str(row["recipient_trader_type"] or "")
        message_type = str(row["message_type"] or "")
        content_json = str(row["content_json"] or "")
        timestamp = str(row["timestamp"] or "")
        created_ts = row["created_ts"]

        yield [
            trading_session_uuid,
            recipient_uuid,
            recipient_type,
            message_type,
            content_json,
            timestamp,
            created_ts,
        ]


def custom_export_transactions(players):
    yield [
        "trading_session_uuid",
        "transaction_id",
        "bid_order_id",
        "ask_order_id",
        "bid_trader_uuid",
        "bid_trader_type",
        "ask_trader_uuid",
        "ask_trader_type",
        "quantity",
        "price",
        "timestamp",
        "transaction_json",
        "created_ts",
    ]

    rows = _fetch_export_rows(
        """
        SELECT tx.trading_session_uuid,
               tx.transaction_id,
               tx.bid_order_id,
               tx.ask_order_id,
               tx.bid_trader_uuid,
               bt.trader_type AS bid_trader_type,
               tx.ask_trader_uuid,
               at.trader_type AS ask_trader_type,
               tx.price,
               tx.timestamp,
               tx.transaction_json,
               tx.created_ts
        FROM trading_platform_transactions AS tx
        LEFT JOIN trading_platform_traders AS bt
          ON bt.trading_session_uuid = tx.trading_session_uuid
         AND bt.trader_uuid = tx.bid_trader_uuid
        LEFT JOIN trading_platform_traders AS at
          ON at.trading_session_uuid = tx.trading_session_uuid
         AND at.trader_uuid = tx.ask_trader_uuid
        ORDER BY tx.id ASC
        """,
        export_name="custom_export_transactions",
        missing_table_hint="trading_platform_transactions unavailable",
    )

    for row in rows:
        quantity = _extract_quantity_from_transaction_json(row["transaction_json"])
        yield [
            str(row["trading_session_uuid"] or ""),
            str(row["transaction_id"] or ""),
            str(row["bid_order_id"] or ""),
            str(row["ask_order_id"] or ""),
            str(row["bid_trader_uuid"] or ""),
            str(row["bid_trader_type"] or ""),
            str(row["ask_trader_uuid"] or ""),
            str(row["ask_trader_type"] or ""),
            quantity,
            row["price"],
            str(row["timestamp"] or ""),
            str(row["transaction_json"] or ""),
            row["created_ts"],
        ]


def custom_export_orders(players):
    yield [
        "trading_session_uuid",
        "trader_uuid",
        "trader_type",
        "order_id",
        "status",
        "order_type",
        "amount",
        "price",
        "timestamp",
        "order_json",
        "created_ts",
    ]

    rows = _fetch_export_rows(
        """
        SELECT o.trading_session_uuid,
               o.trader_uuid,
               t.trader_type AS trader_type,
               o.order_id,
               o.status,
               o.order_type,
               o.amount,
               o.price,
               o.timestamp,
               o.order_json,
               o.created_ts
        FROM trading_platform_orders AS o
        LEFT JOIN trading_platform_traders AS t
          ON t.trading_session_uuid = o.trading_session_uuid
         AND t.trader_uuid = o.trader_uuid
        ORDER BY o.id ASC
        """,
        export_name="custom_export_orders",
        missing_table_hint="trading_platform_orders unavailable",
    )

    for row in rows:
        yield [
            str(row["trading_session_uuid"] or ""),
            str(row["trader_uuid"] or ""),
            str(row["trader_type"] or ""),
            str(row["order_id"] or ""),
            str(row["status"] or ""),
            str(row["order_type"] or ""),
            row["amount"],
            row["price"],
            str(row["timestamp"] or ""),
            str(row["order_json"] or ""),
            row["created_ts"],
        ]

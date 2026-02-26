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
    NUM_ROUNDS = 30

    DEFAULT_TRADING_API_BASE = "http://127.0.0.1:8001"
    DEFAULT_API_TIMEOUT_SECONDS = 20
    DEFAULT_TRADING_DAY_DURATION = 1
    DEFAULT_STEP = 1
    DEFAULT_MAX_ORDERS_PER_MINUTE = 30
    DEFAULT_INITIAL_MIDPOINT = 100
    DEFAULT_INITIAL_SPREAD = 10
    DEFAULT_INITIAL_CASH = 2600
    DEFAULT_INITIAL_STOCKS = 20
    DEFAULT_ALERT_STREAK_FREQUENCY = 3
    DEFAULT_ALERT_WINDOW_SIZE = 5
    DEFAULT_ALLOW_SELF_TRADE = True
    DEFAULT_GROUP_SIZE = 2
    DEFAULT_HYBRID_NOISE_TRADERS = 1
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


class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    trading_session_uuid = models.StringField(blank=True)
    trading_api_base = models.StringField(blank=True)
    trading_ws_base = models.StringField(blank=True)
    trading_init_error = models.LongStringField(blank=True)
    trading_day_duration_minutes = models.IntegerField(initial=C.DEFAULT_TRADING_DAY_DURATION)
    treatment = models.StringField(initial="gh")
    market_design = models.StringField(initial="gamified")
    group_composition = models.StringField(initial="human_only")


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
        subsession.group_like_round(1)
        for group in subsession.get_groups():
            round_1_group = group.in_round(1)
            group.treatment = round_1_group.treatment
            group.market_design = round_1_group.market_design
            group.group_composition = round_1_group.group_composition
        _log("creating_session copied group matrix + treatments from round 1", round_number=subsession.round_number)
        return

    players = subsession.get_players()
    if not players:
        _log("creating_session found no players")
        return

    _log("creating_session players loaded", player_ids=[p.id_in_subsession for p in players], num_players=len(players))

    desired_size = _as_int(subsession.session.config.get("players_per_group", C.DEFAULT_GROUP_SIZE), C.DEFAULT_GROUP_SIZE)
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

    configured_treatments = _parse_treatments(subsession.session.config.get("treatments"))
    groups = subsession.get_groups()
    for idx, group in enumerate(groups):
        treatment = configured_treatments[idx % len(configured_treatments)]
        _set_group_treatment(group, treatment)
    _log(
        "creating_session assigned treatments",
        treatments=[g.treatment for g in groups],
        market_designs=[g.market_design for g in groups],
        group_compositions=[g.group_composition for g in groups],
    )


def _parse_treatments(raw_value):
    if raw_value is None:
        return list(C.TREATMENTS)
    if isinstance(raw_value, str):
        candidate_values = [x.strip().lower() for x in raw_value.split(",")]
    elif isinstance(raw_value, (list, tuple)):
        candidate_values = [str(x).strip().lower() for x in raw_value]
    else:
        return list(C.TREATMENTS)
    filtered = [x for x in candidate_values if x in C.TREATMENTS]
    return filtered or list(C.TREATMENTS)


def _set_group_treatment(group: Group, treatment: str):
    treatment_value = str(treatment or "").strip().lower()
    if treatment_value not in C.TREATMENTS:
        treatment_value = C.TREATMENTS[0]
    group.treatment = treatment_value
    group.market_design = C.TREATMENT_MARKET_DESIGN[treatment_value]
    group.group_composition = C.TREATMENT_GROUP_COMPOSITION[treatment_value]


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
    hybrid_noise_traders = _as_int(
        cfg.get("hybrid_noise_traders", C.DEFAULT_HYBRID_NOISE_TRADERS),
        C.DEFAULT_HYBRID_NOISE_TRADERS,
    )
    # TEMP: force noise traders in all treatments (including "human_only") for debugging/demo runs.
    # Revert to the composition-based condition once we restore treatment-specific behavior.
    num_noise_traders = max(0, hybrid_noise_traders)
    return dict(
        num_human_traders=num_players,
        num_noise_traders=num_noise_traders,
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
        treatment=group.treatment,
        market_design=group.market_design,
        group_composition=group.group_composition,
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
            treatment=player.group.treatment,
            market_design=player.group.market_design,
            group_composition=player.group.group_composition,
        )

    @staticmethod
    def js_vars(player: Player):
        ws_url = f"{player.group.trading_ws_base}/trader/{player.trader_uuid}"
        gamified = player.group.market_design == "gamified"
        return dict(
            wsUrl=ws_url,
            wsBase=player.group.trading_ws_base,
            traderUuid=player.trader_uuid,
            httpUrl=f"{player.group.trading_api_base}/",
            tradingApiBase=player.group.trading_api_base,
            tradingSessionUuid=player.group.trading_session_uuid,
            playerIdInGroup=player.id_in_group,
            gamified=gamified,
            treatment=player.group.treatment,
            marketDesign=player.group.market_design,
            groupComposition=player.group.group_composition,
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


def _to_float_or_none(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int_or_none(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_side(order_type_raw):
    text = str(order_type_raw or "").strip().lower()
    if text in {"1", "bid", "b"}:
        return "bid"
    if text in {"-1", "ask", "a"}:
        return "ask"
    return ""


def _event_time_sort_key(ts_text, created_ts):
    ts_text = str(ts_text or "").strip()
    if ts_text:
        candidate = ts_text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(candidate).timestamp()
        except Exception:
            pass
    created = _to_float_or_none(created_ts)
    return created if created is not None else 0.0


def _fetch_order_snapshot_rows_for_market_exports():
    return _fetch_export_rows(
        """
        SELECT o.id AS row_id,
               o.trading_session_uuid,
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
        export_name="market_export_orders_source",
        missing_table_hint="trading_platform_orders unavailable",
    )


def _fetch_trade_rows_for_market_exports():
    return _fetch_export_rows(
        """
        SELECT tx.id AS row_id,
               tx.trading_session_uuid,
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
        export_name="market_export_trades_source",
        missing_table_hint="trading_platform_transactions unavailable",
    )


def _fetch_persisted_mbo_rows():
    return _fetch_export_rows(
        """
        SELECT trading_session_uuid,
               event_seq,
               event_ts,
               record_kind,
               event_type,
               side,
               order_id,
               trader_uuid,
               price,
               size,
               size_delta,
               size_resting_after,
               status_after,
               match_id,
               contra_order_id,
               bid_order_id,
               ask_order_id,
               bid_trader_uuid,
               ask_trader_uuid,
               created_ts
        FROM trading_platform_mbo_events
        ORDER BY id ASC
        """,
        export_name="custom_export_mbo",
        missing_table_hint="trading_platform_mbo_events unavailable",
    )


def _fetch_persisted_mbp1_rows():
    return _fetch_export_rows(
        """
        SELECT trading_session_uuid,
               event_seq,
               event_ts,
               source_mbo_event_seq,
               source_order_id,
               source_event_type,
               best_bid_px,
               best_bid_sz,
               best_bid_ct,
               best_ask_px,
               best_ask_sz,
               best_ask_ct,
               spread,
               midpoint,
               created_ts
        FROM trading_platform_mbp1_events
        ORDER BY id ASC
        """,
        export_name="custom_export_mbp1",
        missing_table_hint="trading_platform_mbp1_events unavailable",
    )


def _infer_order_event_type(previous_snapshot, current_snapshot):
    status = str((current_snapshot or {}).get("status") or "").strip().lower()
    prev_status = str((previous_snapshot or {}).get("status") or "").strip().lower()
    current_amount = _to_float_or_none((current_snapshot or {}).get("amount"))
    previous_amount = _to_float_or_none((previous_snapshot or {}).get("amount"))
    current_price = _to_float_or_none((current_snapshot or {}).get("price"))
    previous_price = _to_float_or_none((previous_snapshot or {}).get("price"))

    if previous_snapshot is None:
        if status in {"cancelled", "executed"}:
            return status
        return "add"

    if status == "cancelled":
        return "cancel"
    if status == "executed":
        return "fill"
    if current_amount is not None and previous_amount is not None and current_amount < previous_amount - 1e-12:
        return "fill"
    if current_price != previous_price:
        return "modify"
    if current_amount != previous_amount:
        return "modify"
    if status != prev_status:
        return "modify"
    return "state"


def _infer_qty_delta(previous_snapshot, current_snapshot, event_type):
    current_amount = _to_float_or_none((current_snapshot or {}).get("amount"))
    previous_amount = _to_float_or_none((previous_snapshot or {}).get("amount"))

    if previous_snapshot is None:
        return current_amount
    if event_type in {"cancel", "cancelled", "executed"}:
        return -previous_amount if previous_amount is not None else None
    if event_type == "fill":
        if current_amount is not None and previous_amount is not None:
            return current_amount - previous_amount
        return -previous_amount if previous_amount is not None else None
    if event_type == "modify":
        if current_amount is not None and previous_amount is not None:
            return current_amount - previous_amount
    return None


def _order_snapshot_state_dict(row):
    return {
        "status": str(row["status"] or ""),
        "amount": row["amount"],
        "price": row["price"],
        "order_type": str(row["order_type"] or ""),
    }


def _best_levels_from_active_orders(active_orders_by_id):
    bid_levels = {}
    ask_levels = {}

    for order in (active_orders_by_id or {}).values():
        side = _normalize_side(order.get("order_type"))
        price = _to_float_or_none(order.get("price"))
        amount = _to_float_or_none(order.get("amount"))
        if side not in {"bid", "ask"} or price is None or amount is None or amount <= 0:
            continue

        target = bid_levels if side == "bid" else ask_levels
        level = target.setdefault(price, {"size": 0.0, "count": 0})
        level["size"] += amount
        level["count"] += 1

    best_bid_px = max(bid_levels.keys()) if bid_levels else None
    best_ask_px = min(ask_levels.keys()) if ask_levels else None
    best_bid = bid_levels.get(best_bid_px) if best_bid_px is not None else None
    best_ask = ask_levels.get(best_ask_px) if best_ask_px is not None else None

    spread = None
    midpoint = None
    if best_bid_px is not None and best_ask_px is not None:
        spread = best_ask_px - best_bid_px
        midpoint = (best_ask_px + best_bid_px) / 2.0

    return {
        "best_bid_px": best_bid_px,
        "best_bid_sz": (best_bid or {}).get("size"),
        "best_bid_ct": (best_bid or {}).get("count"),
        "best_ask_px": best_ask_px,
        "best_ask_sz": (best_ask or {}).get("size"),
        "best_ask_ct": (best_ask or {}).get("count"),
        "spread": spread,
        "midpoint": midpoint,
    }


def custom_export_mbo(players):
    yield [
        "trading_session_uuid",
        "event_seq",
        "event_ts",
        "record_kind",
        "event_type",
        "side",
        "order_id",
        "trader_uuid",
        "price",
        "size",
        "size_delta",
        "size_resting_after",
        "status_after",
        "match_id",
        "contra_order_id",
        "bid_order_id",
        "ask_order_id",
        "bid_trader_uuid",
        "ask_trader_uuid",
        "source_created_ts",
    ]

    persisted_rows = _fetch_persisted_mbo_rows()
    if persisted_rows:
        for row in persisted_rows:
            yield [
                str(row["trading_session_uuid"] or ""),
                row["event_seq"],
                str(row["event_ts"] or ""),
                str(row["record_kind"] or ""),
                str(row["event_type"] or ""),
                str(row["side"] or ""),
                str(row["order_id"] or ""),
                str(row["trader_uuid"] or ""),
                row["price"],
                row["size"],
                row["size_delta"],
                row["size_resting_after"],
                str(row["status_after"] or ""),
                str(row["match_id"] or ""),
                str(row["contra_order_id"] or ""),
                str(row["bid_order_id"] or ""),
                str(row["ask_order_id"] or ""),
                str(row["bid_trader_uuid"] or ""),
                str(row["ask_trader_uuid"] or ""),
                row["created_ts"],
            ]
        return

    order_rows = _fetch_order_snapshot_rows_for_market_exports()
    trade_rows = _fetch_trade_rows_for_market_exports()

    events = []
    previous_by_order = {}

    for row in order_rows:
        session_uuid = str(row["trading_session_uuid"] or "")
        order_id = str(row["order_id"] or "")
        order_key = (session_uuid, order_id)
        current_snapshot = _order_snapshot_state_dict(row)
        previous_snapshot = previous_by_order.get(order_key)

        event_type = _infer_order_event_type(previous_snapshot, current_snapshot)
        qty_delta = _infer_qty_delta(previous_snapshot, current_snapshot, event_type)
        side = _normalize_side(row["order_type"])
        qty = _to_float_or_none(row["amount"])

        events.append(
            {
                "sort_ts": _event_time_sort_key(row["timestamp"], row["created_ts"]),
                "sort_source": 0,
                "sort_id": _to_int_or_none(row["row_id"]) or 0,
                "trading_session_uuid": session_uuid,
                "event_ts": str(row["timestamp"] or ""),
                "record_kind": "order",
                "event_type": event_type,
                "side": side,
                "order_id": order_id,
                "trader_uuid": str(row["trader_uuid"] or ""),
                "price": row["price"],
                "qty": qty,
                "qty_delta": qty_delta,
                "qty_resting_after": qty,
                "status_after": str(row["status"] or ""),
                "match_id": "",
                "contra_order_id": "",
                "bid_order_id": "",
                "ask_order_id": "",
                "bid_trader_uuid": "",
                "ask_trader_uuid": "",
                "source_created_ts": row["created_ts"],
            }
        )

        previous_by_order[order_key] = current_snapshot

    for row in trade_rows:
        quantity = _extract_quantity_from_transaction_json(row["transaction_json"])
        qty = _to_float_or_none(quantity)
        events.append(
            {
                "sort_ts": _event_time_sort_key(row["timestamp"], row["created_ts"]),
                "sort_source": 1,
                "sort_id": _to_int_or_none(row["row_id"]) or 0,
                "trading_session_uuid": str(row["trading_session_uuid"] or ""),
                "event_ts": str(row["timestamp"] or ""),
                "record_kind": "trade",
                "event_type": "trade",
                "side": "",
                "order_id": "",
                "trader_uuid": "",
                "price": row["price"],
                "qty": qty if qty is not None else quantity,
                "qty_delta": "",
                "qty_resting_after": "",
                "status_after": "",
                "match_id": str(row["transaction_id"] or ""),
                "contra_order_id": "",
                "bid_order_id": str(row["bid_order_id"] or ""),
                "ask_order_id": str(row["ask_order_id"] or ""),
                "bid_trader_uuid": str(row["bid_trader_uuid"] or ""),
                "ask_trader_uuid": str(row["ask_trader_uuid"] or ""),
                "source_created_ts": row["created_ts"],
            }
        )

    events.sort(key=lambda e: (e["sort_ts"], e["sort_source"], e["sort_id"]))

    for idx, event in enumerate(events, start=1):
        yield [
            event["trading_session_uuid"],
            idx,
            event["event_ts"],
            event["record_kind"],
            event["event_type"],
            event["side"],
            event["order_id"],
            event["trader_uuid"],
            event["price"],
            event["qty"],
            event["qty_delta"],
            event["qty_resting_after"],
            event["status_after"],
            event["match_id"],
            event["contra_order_id"],
            event["bid_order_id"],
            event["ask_order_id"],
            event["bid_trader_uuid"],
            event["ask_trader_uuid"],
            event["source_created_ts"],
        ]


def custom_export_mbp1(players):
    yield [
        "trading_session_uuid",
        "event_seq",
        "event_ts",
        "source_mbo_event_seq",
        "source_order_id",
        "source_order_event_type",
        "best_bid_px",
        "best_bid_sz",
        "best_bid_ct",
        "best_ask_px",
        "best_ask_sz",
        "best_ask_ct",
        "spread",
        "midpoint",
        "source_created_ts",
    ]

    persisted_rows = _fetch_persisted_mbp1_rows()
    if persisted_rows:
        for row in persisted_rows:
            yield [
                str(row["trading_session_uuid"] or ""),
                row["event_seq"],
                str(row["event_ts"] or ""),
                row["source_mbo_event_seq"],
                str(row["source_order_id"] or ""),
                str(row["source_event_type"] or ""),
                row["best_bid_px"],
                row["best_bid_sz"],
                row["best_bid_ct"],
                row["best_ask_px"],
                row["best_ask_sz"],
                row["best_ask_ct"],
                row["spread"],
                row["midpoint"],
                row["created_ts"],
            ]
        return

    order_rows = _fetch_order_snapshot_rows_for_market_exports()
    active_orders_by_session = {}
    previous_snapshots_by_order = {}
    previous_bbo_by_session = {}
    event_seq = 0

    for row in order_rows:
        session_uuid = str(row["trading_session_uuid"] or "")
        order_id = str(row["order_id"] or "")
        if not session_uuid or not order_id:
            continue

        order_key = (session_uuid, order_id)
        current_snapshot = _order_snapshot_state_dict(row)
        previous_snapshot = previous_snapshots_by_order.get(order_key)
        source_event_type = _infer_order_event_type(previous_snapshot, current_snapshot)
        previous_snapshots_by_order[order_key] = current_snapshot

        status = str(row["status"] or "").strip().lower()
        amount = _to_float_or_none(row["amount"])
        price = _to_float_or_none(row["price"])
        side = _normalize_side(row["order_type"])

        session_active = active_orders_by_session.setdefault(session_uuid, {})
        if status == "active" and side in {"bid", "ask"} and price is not None and amount is not None and amount > 0:
            session_active[order_id] = {
                "order_type": row["order_type"],
                "price": price,
                "amount": amount,
            }
        else:
            session_active.pop(order_id, None)

        bbo = _best_levels_from_active_orders(session_active)
        bbo_signature = (
            bbo["best_bid_px"],
            bbo["best_bid_sz"],
            bbo["best_bid_ct"],
            bbo["best_ask_px"],
            bbo["best_ask_sz"],
            bbo["best_ask_ct"],
        )
        if previous_bbo_by_session.get(session_uuid) == bbo_signature:
            continue
        previous_bbo_by_session[session_uuid] = bbo_signature

        event_seq += 1
        yield [
            session_uuid,
            event_seq,
            str(row["timestamp"] or ""),
            row["row_id"],
            order_id,
            source_event_type,
            bbo["best_bid_px"],
            bbo["best_bid_sz"],
            bbo["best_bid_ct"],
            bbo["best_ask_px"],
            bbo["best_ask_sz"],
            bbo["best_ask_ct"],
            bbo["spread"],
            bbo["midpoint"],
            row["created_ts"],
        ]

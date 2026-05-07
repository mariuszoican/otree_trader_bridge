import json
import os
import sqlite3
import traceback
from datetime import datetime, timezone
from pathlib import Path

from .constants import C


def _log(message, **context):
    ts = datetime.now(timezone.utc).isoformat()
    if context:
        details = ", ".join(f"{k}={repr(v)}" for k, v in sorted(context.items()))
        print(f"[trader_bridge.export][{ts}] {message} | {details}", flush=True)
    else:
        print(f"[trader_bridge.export][{ts}] {message}", flush=True)


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
            with sqlite3.connect(target) as conn:
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


def _parse_json_object(raw):
    try:
        parsed = json.loads(str(raw or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_quantity_from_transaction_json(transaction_json_raw):
    parsed = _parse_json_object(transaction_json_raw)

    for key in ("quantity", "amount", "fill_amount", "transaction_amount"):
        value = parsed.get(key)
        if value is not None:
            return value
    return ""


def _extract_trading_day_from_json(raw):
    parsed = _parse_json_object(raw)
    value = parsed.get("trading_day")
    return _to_int_or_none(value)


def _market_number_for_round(round_number):
    try:
        day = int(round_number or 0)
    except (TypeError, ValueError):
        return None
    if day <= 0:
        return None
    cumulative = 0
    for idx, market_days in enumerate(C.MARKET_DAYS, start=1):
        cumulative += int(market_days)
        if day <= cumulative:
            return idx
    return len(C.MARKET_DAYS)


def _group_trading_session_uuid(group):
    if group is None:
        return ""
    field_getter = getattr(group, "field_maybe_none", None)
    if callable(field_getter):
        try:
            value = field_getter("trading_session_uuid")
            return str(value or "")
        except Exception:
            return ""
    try:
        return str(getattr(group, "trading_session_uuid", "") or "")
    except Exception:
        return ""


def _market_number_by_session(players):
    mapping = {}
    for player in players or []:
        group = getattr(player, "group", None)
        session_uuid = _group_trading_session_uuid(group)
        if not session_uuid or session_uuid in mapping:
            continue
        round_number = getattr(player, "round_number", None)
        market_number = _market_number_for_round(round_number)
        if market_number is not None:
            mapping[session_uuid] = market_number
    return mapping


def _session_is_simulated_by_uuid():
    rows = _fetch_export_rows(
        """
        SELECT trading_session_uuid, payload_json
        FROM trading_platform_sessions
        ORDER BY id ASC
        """,
        export_name="session_is_simulated_lookup",
        missing_table_hint="trading_platform_sessions unavailable",
    )
    mapping = {}
    for row in rows:
        session_uuid = str(row["trading_session_uuid"] or "")
        if not session_uuid:
            continue
        payload = _parse_json_object(row["payload_json"])
        mapping[session_uuid] = bool(payload.get("is_simulated", False))
    return mapping


def _trading_day_by_mbo_key(mbo_rows):
    mapping = {}
    for row in mbo_rows or []:
        session_uuid = str(row["trading_session_uuid"] or "")
        event_seq = _to_int_or_none(row["event_seq"])
        if not session_uuid or event_seq is None:
            continue
        trading_day = _extract_trading_day_from_json(row["event_json"])
        if trading_day is not None:
            mapping[(session_uuid, event_seq)] = trading_day
    return mapping


def custom_export(players):
    """Default oTree custom export entrypoint: gamification UI events."""
    yield from custom_export_gamification_ui(players)


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


def custom_export_gamification_ui(players):
    yield [
        "trading_session_uuid",
        "trader_uuid",
        "trader_type",
        "event_name",
        "element_type",
        "element_id",
        "element_label",
        "client_ts",
        "server_received_ts",
        "payload_json",
        "created_ts",
    ]

    rows = _fetch_export_rows(
        """
        SELECT g.trading_session_uuid,
               g.trader_uuid,
               t.trader_type,
               g.event_name,
               g.element_type,
               g.element_id,
               g.element_label,
               g.client_ts,
               g.server_received_ts,
               g.payload_json,
               g.created_ts
        FROM trading_platform_gamification_ui_events AS g
        LEFT JOIN trading_platform_traders AS t
          ON t.trading_session_uuid = g.trading_session_uuid
         AND t.trader_uuid = g.trader_uuid
        ORDER BY g.id ASC
        """,
        export_name="custom_export_gamification_ui",
        missing_table_hint="trading_platform_gamification_ui_events unavailable",
    )

    for row in rows:
        yield [
            str(row["trading_session_uuid"] or ""),
            str(row["trader_uuid"] or ""),
            str(row["trader_type"] or ""),
            str(row["event_name"] or ""),
            str(row["element_type"] or ""),
            str(row["element_id"] or ""),
            str(row["element_label"] or ""),
            str(row["client_ts"] or ""),
            str(row["server_received_ts"] or ""),
            str(row["payload_json"] or ""),
            row["created_ts"],
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


def _extract_submission_metrics_from_mbo_event_json(event_json_raw):
    parsed = _parse_json_object(event_json_raw)
    return {
        "queue_position": _to_int_or_none(parsed.get("queue_position")),
        "queue_size": _to_float_or_none(parsed.get("queue_size")),
    }


def _extract_aggressor_side_from_json(payload_json_raw):
    parsed = _parse_json_object(payload_json_raw)
    side = str(parsed.get("aggressor_side") or "").strip().upper()
    if side in {"B", "S"}:
        return side
    return ""


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
               event_json,
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
    market_by_session = _market_number_by_session(players)
    simulated_by_session = _session_is_simulated_by_uuid()
    yield [
        "trading_session_uuid",
        "is_simulated",
        "market_number",
        "trading_day",
        "event_seq",
        "event_ts",
        "record_kind",
        "event_type",
        "side",
        "aggressor_side",
        "order_id",
        "trader_uuid",
        "price",
        "size",
        "size_delta",
        "size_resting_after",
        "queue_position",
        "queue_size",
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
            metrics = _extract_submission_metrics_from_mbo_event_json(row["event_json"])
            record_kind = str(row["record_kind"] or "").strip().lower()
            event_type = str(row["event_type"] or "").strip().lower()
            aggressor_side = ""
            if record_kind == "trade" or event_type == "trade":
                aggressor_side = _extract_aggressor_side_from_json(row["event_json"])
                if not aggressor_side:
                    side_text = str(row["side"] or "").strip().lower()
                    if side_text == "bid":
                        aggressor_side = "B"
                    elif side_text == "ask":
                        aggressor_side = "S"
            session_uuid = str(row["trading_session_uuid"] or "")
            trading_day = _extract_trading_day_from_json(row["event_json"])
            yield [
                session_uuid,
                simulated_by_session.get(session_uuid, False),
                market_by_session.get(session_uuid, ""),
                trading_day if trading_day is not None else "",
                row["event_seq"],
                str(row["event_ts"] or ""),
                str(row["record_kind"] or ""),
                str(row["event_type"] or ""),
                str(row["side"] or ""),
                aggressor_side,
                str(row["order_id"] or ""),
                str(row["trader_uuid"] or ""),
                row["price"],
                row["size"],
                row["size_delta"],
                row["size_resting_after"],
                metrics.get("queue_position") if metrics.get("queue_position") is not None else "",
                metrics.get("queue_size") if metrics.get("queue_size") is not None else "",
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
                "market_number": market_by_session.get(session_uuid, ""),
                "trading_day": "",
                "event_ts": str(row["timestamp"] or ""),
                "record_kind": "order",
                "event_type": event_type,
                "side": side,
                "aggressor_side": "",
                "order_id": order_id,
                "trader_uuid": str(row["trader_uuid"] or ""),
                "price": row["price"],
                "qty": qty,
                "qty_delta": qty_delta,
                "qty_resting_after": qty,
                "queue_position": "",
                "queue_size": "",
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
        aggressor_side = _extract_aggressor_side_from_json(row["transaction_json"])
        trade_side = ""
        if aggressor_side == "B":
            trade_side = "bid"
        elif aggressor_side == "S":
            trade_side = "ask"
        qty = _to_float_or_none(quantity)
        events.append(
            {
                "sort_ts": _event_time_sort_key(row["timestamp"], row["created_ts"]),
                "sort_source": 1,
                "sort_id": _to_int_or_none(row["row_id"]) or 0,
                "trading_session_uuid": str(row["trading_session_uuid"] or ""),
                "market_number": market_by_session.get(str(row["trading_session_uuid"] or ""), ""),
                "trading_day": _extract_trading_day_from_json(row["transaction_json"]) or "",
                "event_ts": str(row["timestamp"] or ""),
                "record_kind": "trade",
                "event_type": "trade",
                "side": trade_side,
                "aggressor_side": aggressor_side,
                "order_id": "",
                "trader_uuid": "",
                "price": row["price"],
                "qty": qty if qty is not None else quantity,
                "qty_delta": "",
                "qty_resting_after": "",
                "queue_position": "",
                "queue_size": "",
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
            simulated_by_session.get(event["trading_session_uuid"], False),
            event["market_number"],
            event["trading_day"],
            idx,
            event["event_ts"],
            event["record_kind"],
            event["event_type"],
            event["side"],
            event["aggressor_side"],
            event["order_id"],
            event["trader_uuid"],
            event["price"],
            event["qty"],
            event["qty_delta"],
            event["qty_resting_after"],
            event["queue_position"],
            event["queue_size"],
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
    market_by_session = _market_number_by_session(players)
    simulated_by_session = _session_is_simulated_by_uuid()
    yield [
        "trading_session_uuid",
        "is_simulated",
        "market_number",
        "trading_day",
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
        mbo_rows = _fetch_persisted_mbo_rows()
        trading_day_by_key = _trading_day_by_mbo_key(mbo_rows)
        for row in persisted_rows:
            session_uuid = str(row["trading_session_uuid"] or "")
            source_seq = _to_int_or_none(row["source_mbo_event_seq"])
            trading_day = trading_day_by_key.get((session_uuid, source_seq), "")
            yield [
                session_uuid,
                simulated_by_session.get(session_uuid, False),
                market_by_session.get(session_uuid, ""),
                trading_day,
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
            simulated_by_session.get(session_uuid, False),
            market_by_session.get(session_uuid, ""),
            "",
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

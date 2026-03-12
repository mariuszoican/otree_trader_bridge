import json
import os
import random
import unittest
from csv import writer as csv_writer
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from otree.api import Bot, Submission

from . import *
from . import export
from .pages import _is_last_round_of_market, _market_number_for_round, _should_elicit_forecast


BOT_EXPORT_ROOT = (
    Path(__file__).resolve().parents[1]
    / f"__temp_bots_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}_{os.getpid()}"
)


def _bot_rng(bot, salt=""):
    return random.Random(f"{bot.session.code}:{bot.participant.code}:{bot.round_number}:{salt}")


def _trader_bridge_players(participant):
    players = []
    for player in participant.get_players():
        group = getattr(player, "group", None)
        if group is None or not hasattr(group, "trading_session_uuid"):
            continue
        players.append(player)
    return players


def _participant_session_uuids(participant):
    session_ids = []
    for player in _trader_bridge_players(participant):
        group = getattr(player, "group", None)
        session_uuid = str(group.field_maybe_none("trading_session_uuid") or "")
        if session_uuid and session_uuid not in session_ids:
            session_ids.append(session_uuid)
    return session_ids


def _filtered_export_rows(export_rows, session_ids):
    header = export_rows[0]
    body = [row for row in export_rows[1:] if row[0] in session_ids]
    return header, body


def _write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv_writer(f)
        writer.writerows(rows)


def _append_csv_rows(path, rows):
    if not rows:
        return
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv_writer(f)
        if not file_exists:
            writer.writerow(rows[0])
        writer.writerows(rows[1:])


def _session_export_rows(session_ids):
    raw_rows = export._fetch_export_rows(
        """
        SELECT trading_session_uuid, payload_json, response_json, created_ts
        FROM trading_platform_sessions
        ORDER BY id ASC
        """,
        export_name="bot_session_export",
        missing_table_hint="trading_platform_sessions unavailable",
    )
    rows = [["trading_session_uuid", "is_simulated", "payload_json", "response_json", "created_ts"]]
    for row in raw_rows:
        session_uuid = str(row["trading_session_uuid"] or "")
        if session_uuid not in session_ids:
            continue
        payload = export._parse_json_object(row["payload_json"])
        rows.append(
            [
                session_uuid,
                bool(payload.get("is_simulated", False)),
                str(row["payload_json"] or ""),
                str(row["response_json"] or ""),
                row["created_ts"],
            ]
        )
    return rows


def _write_bot_export_snapshot(participant, session_ids, mbo_rows, mbp1_rows):
    BOT_EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    session_code = str(getattr(participant.session, "code", "") or "session")
    participant_code = str(getattr(participant, "code", "") or "participant")

    session_rows = _session_export_rows(session_ids)
    _append_csv_rows(BOT_EXPORT_ROOT / "sessions.csv", session_rows)
    _append_csv_rows(BOT_EXPORT_ROOT / "mbo.csv", mbo_rows)
    _append_csv_rows(BOT_EXPORT_ROOT / "mbp1.csv", mbp1_rows)

    readme_lines = [
        "Aggregated bot export for this test run.",
        f"Bot export root: {BOT_EXPORT_ROOT}",
        f"Session code: {session_code}",
        f"Participant code: {participant.code}",
        f"Simulated sessions appended: {max(0, len(session_rows) - 1)}",
        f"MBO rows appended: {max(0, len(mbo_rows) - 1)}",
        f"MBP1 rows appended: {max(0, len(mbp1_rows) - 1)}",
    ]
    with (BOT_EXPORT_ROOT / "README.txt").open("a", encoding="utf-8") as f:
        if f.tell() == 0:
            f.write("One row block below corresponds to one completed bridge bot session.\n\n")
        f.write("\n".join(readme_lines) + "\n\n")


def _assert_simulated_export_rows(participant):
    bridge_players = _trader_bridge_players(participant)
    session_ids = _participant_session_uuids(participant)
    assert session_ids

    mbo_rows = list(export.custom_export_mbo(bridge_players))
    mbo_header, mbo_body = _filtered_export_rows(mbo_rows, session_ids)
    assert mbo_header[1] == "is_simulated"
    assert mbo_body
    assert {row[0] for row in mbo_body} == set(session_ids)
    assert all(row[1] is True for row in mbo_body)

    mbp1_rows = list(export.custom_export_mbp1(bridge_players))
    mbp1_header, mbp1_body = _filtered_export_rows(mbp1_rows, session_ids)
    assert mbp1_header[1] == "is_simulated"
    assert mbp1_body
    assert {row[0] for row in mbp1_body} == set(session_ids)
    assert all(row[1] is True for row in mbp1_body)

    _write_bot_export_snapshot(
        participant,
        session_ids,
        [mbo_header, *mbo_body],
        [mbp1_header, *mbp1_body],
    )


class PlayerBot(Bot):
    def play_round(self):
        num_days = max(1, int(self.session.config.get("num_days", C.DAYS_PER_MARKET) or C.DAYS_PER_MARKET))
        if _should_elicit_forecast(self.round_number, num_days):
            rng = _bot_rng(self, "forecast")
            forecast_price = rng.randint(85, 135)
            forecast_confidence = rng.randint(1, 5)
            forecast_payload = dict(
                forecast_price_next_day=forecast_price,
                forecast_confidence_next_day=forecast_confidence,
                forecast_survey_json=json.dumps(
                    {
                        "forecast_price_next_day": forecast_price,
                        "forecast_confidence_next_day": forecast_confidence,
                    }
                ),
            )
            yield Submission(DayBreak, forecast_payload, check_html=False)
        else:
            yield DayBreak

        if _is_last_round_of_market(self.round_number) and _market_number_for_round(self.round_number) < C.NUM_MARKETS:
            yield MarketTransition

        if self.round_number == C.NUM_ROUNDS:
            assert self.participant.vars.get("payable_market") in range(1, C.NUM_MARKETS + 1)
            assert "payoff_for_trade" in self.participant.vars
            assert "cumulative_bonuses" in self.participant.vars
            if self.player.id_in_group == 1:
                _assert_simulated_export_rows(self.participant)


class ExportTests(unittest.TestCase):
    def test_custom_export_mbo_includes_is_simulated(self):
        session_uuid = "session-sim-1"
        mbo_rows = [
            dict(
                trading_session_uuid=session_uuid,
                event_seq=7,
                event_ts="2026-03-12T15:23:44+00:00",
                record_kind="order",
                event_type="add",
                side="bid",
                order_id="order-1",
                trader_uuid="trader-1",
                price=120.0,
                size=1.0,
                size_delta=1.0,
                size_resting_after=1.0,
                status_after="active",
                match_id="",
                contra_order_id="",
                bid_order_id="",
                ask_order_id="",
                bid_trader_uuid="",
                ask_trader_uuid="",
                event_json=json.dumps({"trading_day": 1, "queue_position": 1, "queue_size": 1}),
                created_ts=123.45,
            )
        ]
        with patch.object(export, "_market_number_by_session", return_value={session_uuid: 2}), patch.object(
            export, "_session_is_simulated_by_uuid", return_value={session_uuid: True}
        ), patch.object(export, "_fetch_persisted_mbo_rows", return_value=mbo_rows):
            rows = list(export.custom_export_mbo([]))

        assert rows[0][1] == "is_simulated"
        assert rows[1][0] == session_uuid
        assert rows[1][1] is True
        assert rows[1][2] == 2
        assert rows[1][3] == 1

    def test_custom_export_mbp1_includes_is_simulated(self):
        session_uuid = "session-sim-2"
        mbp1_rows = [
            dict(
                trading_session_uuid=session_uuid,
                event_seq=3,
                event_ts="2026-03-12T15:23:44+00:00",
                source_mbo_event_seq=7,
                source_order_id="order-1",
                source_event_type="add",
                best_bid_px=120.0,
                best_bid_sz=1.0,
                best_bid_ct=1,
                best_ask_px=130.0,
                best_ask_sz=1.0,
                best_ask_ct=1,
                spread=10.0,
                midpoint=125.0,
                created_ts=223.45,
            )
        ]
        mbo_rows = [
            dict(
                trading_session_uuid=session_uuid,
                event_seq=7,
                event_json=json.dumps({"trading_day": 2}),
            )
        ]
        with patch.object(export, "_market_number_by_session", return_value={session_uuid: 1}), patch.object(
            export, "_session_is_simulated_by_uuid", return_value={session_uuid: True}
        ), patch.object(export, "_fetch_persisted_mbp1_rows", return_value=mbp1_rows), patch.object(
            export, "_fetch_persisted_mbo_rows", return_value=mbo_rows
        ):
            rows = list(export.custom_export_mbp1([]))

        assert rows[0][1] == "is_simulated"
        assert rows[1][0] == session_uuid
        assert rows[1][1] is True
        assert rows[1][2] == 1
        assert rows[1][3] == 2

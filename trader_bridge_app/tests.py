import json
import unittest
from csv import writer as csv_writer
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from otree.api import Bot, Submission

from . import *
from . import pages as bridge_pages
from . import export
from .pages import _is_last_round_of_market, _market_number_for_round, _should_elicit_forecast


def _participant_session_uuids(participant):
    session_ids = []
    for player in participant.get_players():
        group = getattr(player, "group", None)
        if group is None:
            continue
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
    root = Path(__file__).resolve().parents[1]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = root / f"_bots_{timestamp}"
    suffix = 1
    while output_dir.exists():
        output_dir = root / f"_bots_{timestamp}_{suffix}"
        suffix += 1
    output_dir.mkdir(parents=True, exist_ok=False)

    session_rows = _session_export_rows(session_ids)
    _write_csv(output_dir / "sessions.csv", session_rows)
    _write_csv(output_dir / "mbo.csv", mbo_rows)
    _write_csv(output_dir / "mbp1.csv", mbp1_rows)

    readme_lines = [
        f"Participant code: {participant.code}",
        f"Simulated sessions exported: {max(0, len(session_rows) - 1)}",
        f"MBO rows exported: {max(0, len(mbo_rows) - 1)}",
        f"MBP1 rows exported: {max(0, len(mbp1_rows) - 1)}",
    ]
    (output_dir / "README.txt").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")


def _assert_simulated_export_rows(participant):
    session_ids = _participant_session_uuids(participant)
    assert session_ids

    mbo_rows = list(export.custom_export_mbo(participant.get_players()))
    mbo_header, mbo_body = _filtered_export_rows(mbo_rows, session_ids)
    assert mbo_header[1] == "is_simulated"
    assert mbo_body
    assert {row[0] for row in mbo_body} == set(session_ids)
    assert all(row[1] is True for row in mbo_body)

    mbp1_rows = list(export.custom_export_mbp1(participant.get_players()))
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
            forecast_payload = dict(
                forecast_price_next_day=100 + self.round_number,
                forecast_confidence_next_day=3,
                forecast_survey_json=json.dumps(
                    {
                        "forecast_price_next_day": 100 + self.round_number,
                        "forecast_confidence_next_day": 3,
                    }
                ),
            )
            yield Submission(DayBreak, forecast_payload, check_html=False)
        else:
            yield DayBreak

        if _is_last_round_of_market(self.round_number) and str(self.player.group.group_composition or "").strip().lower() == "hybrid":
            yield AlgoBeliefAfterMarket, dict(
                algo_belief_present="yes",
                algo_belief_confidence=4,
            )

        if self.round_number == C.NUM_ROUNDS:
            assert self.participant.vars.get("payable_market") in range(1, C.NUM_MARKETS + 1)
            assert "payoff_for_trade" in self.participant.vars
            assert "cumulative_bonuses" in self.participant.vars
            if self.player.id_in_group == 1:
                _assert_simulated_export_rows(self.participant)


class TransitionSideEffectTests(unittest.TestCase):
    def test_daybreak_template_does_not_run_transition_side_effects(self):
        group = SimpleNamespace(
            field_maybe_none=lambda name: C.DAYS_PER_MARKET if name == "num_days" else None,
        )
        player = SimpleNamespace(
            round_number=C.DAYS_PER_MARKET,
            group=group,
            current_cash=100.0,
            num_shares=2.0,
            dividend_per_share=4.0,
            dividend_cash=8.0,
            cash_after_dividend=108.0,
            daybreak_snapshot_error="",
            field_maybe_none=lambda name: None,
        )

        with patch.object(bridge_pages, "_copy_market_start_trading_state") as copy_state, patch.object(
            bridge_pages, "_finalize_trading_session"
        ) as finalize, patch.object(bridge_pages, "_capture_daybreak_state") as capture:
            context = bridge_pages.DayBreak.vars_for_template(player)

        copy_state.assert_not_called()
        finalize.assert_not_called()
        capture.assert_not_called()
        assert context["is_final_day"] is True

    def test_results_template_does_not_copy_state_or_fetch_live_trader_info(self):
        group = SimpleNamespace(trading_api_base="https://trading.example")
        player = SimpleNamespace(
            round_number=C.DAYS_PER_MARKET,
            group=group,
            trader_uuid="trader-1",
            assigned_initial_cash=100.0,
            field_maybe_none=lambda name: {
                "cash_after_dividend": 125.0,
                "num_shares": 3.0,
            }.get(name),
        )

        with patch.object(bridge_pages, "_copy_market_start_trading_state") as copy_state, patch.object(
            bridge_pages, "_fetch_trader_info"
        ) as fetch_info:
            context = bridge_pages.Results.vars_for_template(player)

        copy_state.assert_not_called()
        fetch_info.assert_not_called()
        assert context["final_cash"] == 125.0
        assert context["final_total_shares"] == 3.0

    def test_final_day_transition_has_dedicated_wait_page(self):
        assert hasattr(bridge_pages, "FinalizeTradingSession")

        group = SimpleNamespace(
            trading_session_uuid="session-1",
            trading_api_base="https://trading.example",
            trading_init_error="",
            field_maybe_none=lambda name: "",
            get_players=lambda: [SimpleNamespace(id_in_group=1), SimpleNamespace(id_in_group=2)],
            subsession=SimpleNamespace(round_number=C.DAYS_PER_MARKET),
            session=SimpleNamespace(code="session-code"),
            id=7,
        )
        player = SimpleNamespace(round_number=C.DAYS_PER_MARKET, group=group)

        assert bridge_pages.FinalizeTradingSession.is_displayed(player) is True
        with patch.object(bridge_pages, "_copy_market_start_trading_state") as copy_state, patch.object(
            bridge_pages, "_finalize_trading_session", return_value={"status": "closed"}
        ) as finalize, patch.object(bridge_pages, "_capture_daybreak_state") as capture:
            bridge_pages.finalize_trading_after_wait(group)

        copy_state.assert_called_once_with(group)
        finalize.assert_called_once_with(group)
        capture.assert_called_once_with(group)


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


class AlgoBeliefPageTests(unittest.TestCase):
    def test_algo_belief_page_visible_only_for_last_round_hybrid(self):
        player = SimpleNamespace(round_number=C.DAYS_PER_MARKET, group=SimpleNamespace(group_composition="hybrid"))
        assert AlgoBeliefAfterMarket.is_displayed(player) is True

        player = SimpleNamespace(round_number=1, group=SimpleNamespace(group_composition="hybrid"))
        assert AlgoBeliefAfterMarket.is_displayed(player) is False

        player = SimpleNamespace(round_number=C.DAYS_PER_MARKET, group=SimpleNamespace(group_composition="human_only"))
        assert AlgoBeliefAfterMarket.is_displayed(player) is False

    def test_algo_belief_page_requires_valid_inputs(self):
        player = SimpleNamespace()
        assert (
            AlgoBeliefAfterMarket.error_message(
                player,
                {"algo_belief_present": "", "algo_belief_confidence": None},
            )
            == "Please indicate whether you believe an algorithmic trader was present."
        )
        assert (
            AlgoBeliefAfterMarket.error_message(
                player,
                {"algo_belief_present": "yes", "algo_belief_confidence": 7},
            )
            == "Confidence must be between 1 and 5."
        )
        assert (
            AlgoBeliefAfterMarket.error_message(
                player,
                {"algo_belief_present": "no", "algo_belief_confidence": 3},
            )
            is None
        )

import ast
import json
import os
import traceback

from otree.api import WaitPage
from otree.api import Page as oTreePage

from .constants import C
from .models import Group, Player, Subsession
from .utils import (
    _as_bool,
    _as_float,
    _as_int,
    _get_json,
    _log,
    _normalize_http_base,
    _post_json,
    _resolve_day_duration_minutes,
    _ws_base_from_http,
)

def _format_number(value):
    value = _as_float(value, 0.0)
    if float(value).is_integer():
        return str(int(value))
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _as_number_list(raw, fallback):
    if isinstance(raw, (list, tuple)):
        values = []
        for item in raw:
            try:
                values.append(float(item))
            except (TypeError, ValueError):
                continue
        return values or list(fallback)

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return list(fallback)
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except Exception:
                continue
            if isinstance(parsed, (list, tuple)):
                return _as_number_list(parsed, fallback)
        parts = [x.strip() for x in text.split(",") if x.strip()]
        values = []
        for part in parts:
            try:
                values.append(float(part))
            except (TypeError, ValueError):
                continue
        return values or list(fallback)
    return list(fallback)


def _money(value):
    return f"E${_format_number(value)}"


def _money_series_text(values):
    shown = [_money(v) for v in values]
    if not shown:
        return ""
    if len(shown) == 1:
        return shown[0]
    if len(shown) == 2:
        return f"{shown[0]} or {shown[1]}"
    return ", ".join(shown[:-1]) + f", or {shown[-1]}"


def _natural_join(items):
    parts = [str(x) for x in items if str(x).strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} or {parts[1]}"
    return ", ".join(parts[:-1]) + f", or {parts[-1]}"


def _format_endowment_options_text(options):
    if not options:
        return ""
    entries = []
    for cash, shares in options:
        entries.append(f"{int(_as_int(shares, 0))} shares and {_money(cash)} in cash")
    return _natural_join(entries)


def _instruction_context(player):
    cfg = player.session.config
    num_markets = max(1, _as_int(cfg.get("num_markets", C.NUM_MARKETS), C.NUM_MARKETS))
    num_days = max(1, _as_int(cfg.get("num_days", C.DAYS_PER_MARKET), C.DAYS_PER_MARKET))
    num_human_traders = max(1, _as_int(cfg.get("players_per_group", C.DEFAULT_GROUP_SIZE), C.DEFAULT_GROUP_SIZE))
    other_human_traders = max(0, num_human_traders - 1)
    day_duration = _resolve_day_duration_minutes(cfg, C.DEFAULT_TRADING_DAY_DURATION)
    market_total_minutes = num_days * day_duration
    forecast_bonus_amount = _as_float(cfg.get("forecast_bonus_amount", C.DEFAULT_FORECAST_BONUS_AMOUNT), C.DEFAULT_FORECAST_BONUS_AMOUNT)
    forecast_bonus_threshold_pct = _as_float(
        cfg.get("forecast_bonus_threshold_pct", C.DEFAULT_FORECAST_BONUS_THRESHOLD_PCT),
        C.DEFAULT_FORECAST_BONUS_THRESHOLD_PCT,
    )
    dividend_values = _as_number_list(
        cfg.get("dividend_values", cfg.get("dividends", C.DEFAULT_DIVIDEND_VALUES)),
        C.DEFAULT_DIVIDEND_VALUES,
    )
    unique_dividend_values = sorted(set(dividend_values))
    shown_dividends = unique_dividend_values[:4] if len(unique_dividend_values) >= 4 else list(C.DEFAULT_DIVIDEND_VALUES)
    expected_dividend = sum(shown_dividends) / max(1, len(shown_dividends))
    fundamental_value_start = expected_dividend * num_days
    fundamental_value_last = expected_dividend
    group_composition = str(getattr(getattr(player, "group", None), "group_composition", "") or "").strip().lower()
    has_algorithmic_traders = group_composition == "hybrid"
    endowment_options = _parse_endowment_options(cfg.get("human_trader_endowments"))
    exchange_rate = _as_float(cfg.get("real_world_currency_per_point", 1), 1)
    quiz_bonus_per_correct = _as_float(cfg.get("fee_per_correct_answer", 1), 1)
    return dict(
        num_human_traders=num_human_traders,
        other_human_traders=other_human_traders,
        has_algorithmic_traders=has_algorithmic_traders,
        num_markets=num_markets,
        num_days=num_days,
        total_periods=num_markets * num_days,
        trading_day_duration=day_duration,
        market_total_minutes=market_total_minutes,
        endowment_options_text=_format_endowment_options_text(endowment_options),
        expected_dividend=_money(expected_dividend),
        fundamental_value_start=_money(fundamental_value_start),
        fundamental_value_step=_money(expected_dividend),
        fundamental_value_last=_money(fundamental_value_last),
        forecast_bonus_amount=_format_number(forecast_bonus_amount),
        forecast_bonus_threshold_pct=_format_number(forecast_bonus_threshold_pct),
        dividend_values_text=_money_series_text(shown_dividends),
        payoff_period=num_days,
        exchange_rate_text=_format_number(exchange_rate),
        quiz_bonus_per_correct_text=_format_number(quiz_bonus_per_correct),
    )



class Page(oTreePage):
    instructions = True

    def get_context_data(self, **context):
        r = super().get_context_data(**context)
        max_pages = int(getattr(self.participant, "_max_page_index", 1) or 1)
        page_index = int(getattr(self, "_index_in_pages", 1) or 1)
        progress = int(page_index / max_pages * 100) if max_pages > 0 else 0
        progress = max(0, min(100, progress))
        r.update(
            dict(
                maxpages=max_pages,
                page_index=page_index,
                progress=f"{progress:d}",
                instructions=self.instructions,
            )
        )
        r.update(_instruction_context(self.player))
        return r


def creating_session(subsession: Subsession):
    _validate_market_structure()
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
            for player in group.get_players():
                round_1_player = player.in_round(1)
                player.assigned_initial_cash = _as_float(
                    round_1_player.assigned_initial_cash,
                    _as_float(player.participant.vars.get("assigned_initial_cash"), C.DEFAULT_INITIAL_CASH),
                )
                player.assigned_initial_shares = _as_float(
                    round_1_player.assigned_initial_shares,
                    _as_float(player.participant.vars.get("assigned_initial_shares"), C.DEFAULT_INITIAL_STOCKS),
                )
                player.participant.vars["assigned_initial_cash"] = player.assigned_initial_cash
                player.participant.vars["assigned_initial_shares"] = player.assigned_initial_shares
        _log("creating_session copied group matrix + treatments from round 1", round_number=subsession.round_number)
        return

    players = subsession.get_players()
    if not players:
        _log("creating_session found no players")
        return

    _log("creating_session players loaded", player_ids=[p.id_in_subsession for p in players], num_players=len(players))

    configured_treatments = _parse_treatments(subsession.session.config.get("treatments"))
    groups = subsession.get_groups()
    for idx, group in enumerate(groups):
        players_in_group = group.get_players()
        intro_treatment = ""
        if players_in_group:
            intro_treatment = str(players_in_group[0].participant.vars.get("treatment", "") or "").strip().lower()
        if intro_treatment in C.TREATMENTS:
            _set_group_treatment(group, intro_treatment)
        else:
            treatment = configured_treatments[idx % len(configured_treatments)]
            _set_group_treatment(group, treatment)
        for player in players_in_group:
            player.participant.vars["treatment"] = group.treatment
            player.participant.vars["market_design"] = group.market_design
            player.participant.vars["group_composition"] = group.group_composition
        _assign_player_endowments(group)
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


def _expected_total_rounds():
    return int(C.NUM_MARKETS * C.DAYS_PER_MARKET)


def _validate_market_structure():
    expected = _expected_total_rounds()
    if int(C.NUM_ROUNDS) != expected:
        raise RuntimeError(
            f"NUM_ROUNDS={C.NUM_ROUNDS} must equal NUM_MARKETS*DAYS_PER_MARKET={expected}."
        )


def _market_number_for_round(round_number):
    day = max(1, _as_int(round_number, 1))
    return ((day - 1) // C.DAYS_PER_MARKET) + 1


def _day_in_market(round_number):
    day = max(1, _as_int(round_number, 1))
    return ((day - 1) % C.DAYS_PER_MARKET) + 1


def _market_start_round(round_number):
    market_number = _market_number_for_round(round_number)
    return ((market_number - 1) * C.DAYS_PER_MARKET) + 1


def _is_first_round_of_market(round_number):
    return _day_in_market(round_number) == 1


def _is_last_round_of_market(round_number):
    return _day_in_market(round_number) == C.DAYS_PER_MARKET


def _resolve_num_days(cfg):
    num_days = max(1, _as_int(cfg.get("num_days", C.DEFAULT_NUM_DAYS), C.DEFAULT_NUM_DAYS))
    if num_days != C.DAYS_PER_MARKET:
        raise RuntimeError(
            f"Session config num_days={num_days} must equal DAYS_PER_MARKET={C.DAYS_PER_MARKET}."
        )
    return num_days


def _get_group_dividend_schedule(group: Group):
    raw = str(group.dividends_csv or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [_as_float(x, 0.0) for x in parsed]
        except Exception:
            pass
    return [float(x) for x in C.DIVIDEND_SCHEDULE]


def _parse_endowment_options(raw_value):
    if not isinstance(raw_value, (list, tuple)):
        return list(C.DEFAULT_HUMAN_TRADER_ENDOWMENTS)
    parsed = []
    for item in raw_value:
        if isinstance(item, dict):
            cash = _as_float(item.get("initial_cash", item.get("cash")), 0.0)
            shares = max(0, _as_int(item.get("initial_stocks", item.get("shares")), 0))
            parsed.append((cash, shares))
            continue
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            cash = _as_float(item[0], 0.0)
            shares = max(0, _as_int(item[1], 0))
            parsed.append((cash, shares))
    return parsed or list(C.DEFAULT_HUMAN_TRADER_ENDOWMENTS)


def _assign_player_endowments(group: Group):
    options = _parse_endowment_options(group.session.config.get("human_trader_endowments"))
    players = sorted(group.get_players(), key=lambda p: p.id_in_group)
    for idx, player in enumerate(players):
        cash, shares = options[idx % len(options)]
        player.assigned_initial_cash = float(cash)
        player.assigned_initial_shares = float(shares)
        player.participant.vars["assigned_initial_cash"] = player.assigned_initial_cash
        player.participant.vars["assigned_initial_shares"] = player.assigned_initial_shares


def _build_initiate_payload(group: Group, players):
    cfg = group.session.config
    num_players = len(players)
    hybrid_noise_traders = _as_int(
        cfg.get("hybrid_noise_traders", C.DEFAULT_HYBRID_NOISE_TRADERS),
        C.DEFAULT_HYBRID_NOISE_TRADERS,
    )
    # TEMP: force noise traders in all treatments (including "human_only") for debugging/demo runs.
    # Revert to the composition-based condition once we restore treatment-specific behavior.
    num_noise_traders = max(0, hybrid_noise_traders)
    num_days = _resolve_num_days(cfg)
    day_duration_minutes = _resolve_day_duration_minutes(cfg, C.DEFAULT_TRADING_DAY_DURATION)
    all_dividends = [float(x) for x in C.DIVIDEND_SCHEDULE]
    required_days = C.NUM_ROUNDS
    if len(all_dividends) < required_days:
        raise RuntimeError(
            f"Dividend schedule has {len(all_dividends)} values but requires at least {required_days}."
        )
    all_dividends = all_dividends[:required_days]
    market_number = _market_number_for_round(group.subsession.round_number)
    market_start_idx = (market_number - 1) * C.DAYS_PER_MARKET
    market_end_idx = market_start_idx + num_days
    dividends = all_dividends[market_start_idx:market_end_idx]
    if len(dividends) < num_days:
        raise RuntimeError(
            f"Dividend schedule slice for market {market_number} has {len(dividends)} values but needs {num_days}."
        )
    group.num_days = num_days
    group.dividends_csv = json.dumps(all_dividends)
    human_trader_params = [
        {
            "initial_cash": float(_as_float(player.assigned_initial_cash, C.DEFAULT_INITIAL_CASH)),
            "initial_stocks": int(_as_int(player.assigned_initial_shares, C.DEFAULT_INITIAL_STOCKS)),
        }
        for player in players
    ]
    return dict(
        num_human_traders=num_players,
        num_noise_traders=num_noise_traders,
        num_days=num_days,
        dividends=dividends,
        human_trader_params=human_trader_params,
        # Backend computes total market duration as num_days * trading_day_duration.
        trading_day_duration=max(1, day_duration_minutes),
        step=_as_int(
            cfg.get("step", C.DEFAULT_STEP),
            C.DEFAULT_STEP,
        ),
        max_orders_per_minute=_as_int(
            cfg.get("max_orders_per_minute", C.DEFAULT_MAX_ORDERS_PER_MINUTE),
            C.DEFAULT_MAX_ORDERS_PER_MINUTE,
        ),
        noise_trader_start_second=_as_int(
            cfg.get("noise_trader_start_second", C.DEFAULT_NOISE_TRADER_START_SECOND),
            C.DEFAULT_NOISE_TRADER_START_SECOND,
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
    )


def _pause_trading_session(group: Group):
    if not group.trading_session_uuid or not group.trading_api_base:
        raise RuntimeError("Cannot pause: missing trading session UUID or API base.")
    cfg = group.session.config
    timeout_seconds = _as_int(
        cfg.get("trading_api_timeout_seconds", C.DEFAULT_API_TIMEOUT_SECONDS),
        C.DEFAULT_API_TIMEOUT_SECONDS,
    )
    pause_url = f"{group.trading_api_base}/trading_session/{group.trading_session_uuid}/pause"
    response = _post_json(pause_url, {}, timeout_seconds)
    return response.get("data") or {}


def _resume_trading_session(group: Group):
    if not group.trading_session_uuid or not group.trading_api_base:
        raise RuntimeError("Cannot resume: missing trading session UUID or API base.")
    cfg = group.session.config
    timeout_seconds = _as_int(
        cfg.get("trading_api_timeout_seconds", C.DEFAULT_API_TIMEOUT_SECONDS),
        C.DEFAULT_API_TIMEOUT_SECONDS,
    )
    resume_url = f"{group.trading_api_base}/trading_session/{group.trading_session_uuid}/resume"
    response = _post_json(resume_url, {}, timeout_seconds)
    return response.get("data") or {}


def _group_init_error(group: Group) -> str:
    return str(group.field_maybe_none("trading_init_error") or "")


def _copy_market_start_trading_state(group: Group):
    source_round = _market_start_round(group.subsession.round_number)
    source_group = group.in_round(source_round)
    group.trading_session_uuid = source_group.trading_session_uuid
    group.trading_api_base = source_group.trading_api_base
    group.trading_ws_base = source_group.trading_ws_base
    group.trading_day_duration_minutes = source_group.trading_day_duration_minutes
    group.num_days = source_group.num_days
    group.dividends_csv = source_group.dividends_csv
    group.trading_init_error = _group_init_error(source_group)
    for player in group.get_players():
        source_player = player.in_round(source_round)
        player.trader_uuid = source_player.trader_uuid or str(player.participant.vars.get("trader_uuid") or "")
        player.assigned_initial_cash = _as_float(
            source_player.assigned_initial_cash,
            _as_float(player.participant.vars.get("assigned_initial_cash"), C.DEFAULT_INITIAL_CASH),
        )
        player.assigned_initial_shares = _as_float(
            source_player.assigned_initial_shares,
            _as_float(player.participant.vars.get("assigned_initial_shares"), C.DEFAULT_INITIAL_STOCKS),
        )
        player.participant.vars["assigned_initial_cash"] = player.assigned_initial_cash
        player.participant.vars["assigned_initial_shares"] = player.assigned_initial_shares
        if player.trader_uuid:
            player.participant.vars["trader_uuid"] = player.trader_uuid


def _copy_round_1_trading_state(group: Group):
    # Backward-compatible alias used by older call sites.
    _copy_market_start_trading_state(group)


def _fetch_trader_info(group: Group, trader_uuid: str):
    trader_id = str(trader_uuid or "").strip()
    if not trader_id or not group.trading_api_base:
        return {}
    cfg = group.session.config
    timeout_seconds = _as_int(
        cfg.get("trading_api_timeout_seconds", C.DEFAULT_API_TIMEOUT_SECONDS),
        C.DEFAULT_API_TIMEOUT_SECONDS,
    )
    response = _get_json(f"{group.trading_api_base}/trader_info/{trader_id}", timeout_seconds)
    return response.get("data") or {}


def _capture_daybreak_state(group: Group):
    completed_day = int(group.subsession.round_number)
    dividends = _get_group_dividend_schedule(group)
    if len(dividends) < completed_day:
        raise RuntimeError(
            f"Dividend schedule has {len(dividends)} values but day {completed_day} was requested."
        )
    dividend_per_share = _as_float(dividends[completed_day - 1], 0.0)
    for player in group.get_players():
        trader_id = str(player.trader_uuid or "").strip()
        if not trader_id:
            continue
        snapshot_error = ""
        snapshot = {}
        try:
            snapshot = _fetch_trader_info(group, trader_id)
        except Exception as exc:
            snapshot_error = str(exc)
            _log(
                "_capture_daybreak_state trader_info failed",
                round_number=completed_day,
                player_id=player.id_in_subsession,
                trader_uuid=trader_id,
                error=snapshot_error,
            )
        player.num_shares = _as_float(snapshot.get("shares", 0), 0.0)
        player.dividend_per_share = dividend_per_share
        player.dividend_cash = _as_float(player.num_shares * player.dividend_per_share, 0.0)
        player.cash_after_dividend = _as_float(snapshot.get("cash", 0), 0.0)
        player.current_cash = _as_float(player.cash_after_dividend - player.dividend_cash, 0.0)
        player.daybreak_snapshot_error = snapshot_error

        _log(
            "_capture_daybreak_state stored player daybreak values",
            round_number=completed_day,
            player_id=player.id_in_subsession,
            trader_uuid=trader_id,
            current_cash=player.current_cash,
            num_shares=player.num_shares,
            dividend_per_share=player.dividend_per_share,
            dividend_cash=player.dividend_cash,
            cash_after_dividend=player.cash_after_dividend,
            snapshot_error=snapshot_error,
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
        cfg.get("trading_api_base", os.getenv("TRADING_API_BASE", C.DEFAULT_TRADING_API_BASE)),
        C.DEFAULT_TRADING_API_BASE,
    )
    timeout_seconds = _as_int(
        cfg.get("trading_api_timeout_seconds", C.DEFAULT_API_TIMEOUT_SECONDS),
        C.DEFAULT_API_TIMEOUT_SECONDS,
    )

    group.trading_api_base = http_base
    group.trading_ws_base = _ws_base_from_http(http_base)
    group.trading_day_duration_minutes = _resolve_day_duration_minutes(cfg, C.DEFAULT_TRADING_DAY_DURATION)
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
        payload = _build_initiate_payload(group, players)
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

    @staticmethod
    def is_displayed(player: Player):
        return _is_first_round_of_market(player.round_number)


def resume_trading_after_wait(group: Group):
    _copy_market_start_trading_state(group)
    if _group_init_error(group):
        return
    if not group.trading_session_uuid:
        group.trading_init_error = "Missing market-start trading session UUID; cannot resume."
        return
    try:
        result = _resume_trading_session(group)
        _log(
            "resume_trading_after_wait succeeded",
            round_number=group.subsession.round_number,
            trading_session_uuid=group.trading_session_uuid,
            result=result,
        )
        group.trading_init_error = ""
    except Exception as exc:
        group.trading_init_error = str(exc)
        _log(
            "resume_trading_after_wait failed",
            round_number=group.subsession.round_number,
            trading_session_uuid=group.trading_session_uuid,
            error=str(exc),
            traceback=traceback.format_exc(),
        )


def pause_trading_after_wait(group: Group):
    _copy_market_start_trading_state(group)
    if _group_init_error(group):
        return
    if not group.trading_session_uuid:
        group.trading_init_error = "Missing trading session UUID; cannot pause."
        return
    try:
        result = _pause_trading_session(group)
        _capture_daybreak_state(group)
        _log(
            "pause_trading_after_wait succeeded",
            round_number=group.subsession.round_number,
            trading_session_uuid=group.trading_session_uuid,
            result=result,
        )
        group.trading_init_error = ""
    except Exception as exc:
        group.trading_init_error = str(exc)
        _log(
            "pause_trading_after_wait failed",
            round_number=group.subsession.round_number,
            trading_session_uuid=group.trading_session_uuid,
            error=str(exc),
            traceback=traceback.format_exc(),
        )


class PauseTradingSession(WaitPage):
    title_text = "Pausing Market"
    body_text = "Please wait while the market is paused for the intermission."
    after_all_players_arrive = pause_trading_after_wait

    @staticmethod
    def is_displayed(player: Player):
        return (
            not _is_last_round_of_market(player.round_number)
            and not _group_init_error(player.group)
            and bool(player.trader_uuid)
        )


class ResumeTradingSession(WaitPage):
    title_text = "Waiting To Resume Market"
    body_text = "Please wait for all participants to arrive. Trading will resume once everyone is ready."
    after_all_players_arrive = resume_trading_after_wait

    @staticmethod
    def is_displayed(player: Player):
        return player.round_number > 1 and not _is_first_round_of_market(player.round_number)


class InitFailed(Page):
    instructions = False

    @staticmethod
    def is_displayed(player: Player):
        return bool(_group_init_error(player.group))

    @staticmethod
    def vars_for_template(player: Player):
        return dict(
            error_message=_group_init_error(player.group),
            trading_api_base=player.group.trading_api_base,
        )


class TradePage(Page):
    use_standard_layout = False

    @staticmethod
    def is_displayed(player: Player):
        return not _group_init_error(player.group) and bool(player.trader_uuid)

    @staticmethod
    def vars_for_template(player: Player):
        ws_url = f"{player.group.trading_ws_base}/trader/{player.trader_uuid}"
        data = dict(
            ws_url=ws_url,
            trader_uuid=player.trader_uuid,
            trading_api_base=player.group.trading_api_base,
            trading_session_uuid=player.group.trading_session_uuid,
            treatment=player.group.treatment,
            market_design=player.group.market_design,
            group_composition=player.group.group_composition,
        )
        data.update(_instruction_context(player))
        return data

    @staticmethod
    def js_vars(player: Player):
        ws_url = f"{player.group.trading_ws_base}/trader/{player.trader_uuid}"
        gamified = player.group.market_design == "gamified"
        day_duration_minutes = _resolve_day_duration_minutes(player.session.config, C.DEFAULT_TRADING_DAY_DURATION)
        market_number = _market_number_for_round(player.round_number)
        day_in_market = _day_in_market(player.round_number)
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
            marketNumber=market_number,
            totalMarkets=C.NUM_MARKETS,
            roundNumber=day_in_market,
            totalRounds=C.DAYS_PER_MARKET,
            dayDurationMinutes=day_duration_minutes,
        )

    @staticmethod
    def get_timeout_seconds(player: Player):
        duration_minutes = _resolve_day_duration_minutes(player.session.config, C.DEFAULT_TRADING_DAY_DURATION)
        # Keep day timeout equal to configured day duration for every trading day.
        return max(15, int(duration_minutes * 60))


class DayBreak(Page):
    form_model = "player"
    form_fields = ["forecast_price_next_day", "forecast_confidence_next_day", "forecast_survey_json"]

    @staticmethod
    def is_displayed(player: Player):
        return (
            not _group_init_error(player.group)
            and bool(player.trader_uuid)
        )

    @staticmethod
    def get_form_fields(player: Player):
        if _is_last_round_of_market(player.round_number):
            return []
        return ["forecast_price_next_day", "forecast_confidence_next_day", "forecast_survey_json"]

    @staticmethod
    def vars_for_template(player: Player):
        _copy_market_start_trading_state(player.group)
        market_number = _market_number_for_round(player.round_number)
        completed_day = _day_in_market(player.round_number)
        is_final_day = _is_last_round_of_market(player.round_number)
        if is_final_day:
            # On the last day there is no pause wait page, so capture final snapshot here.
            _capture_daybreak_state(player.group)
        return dict(
            market_number=market_number,
            completed_day=completed_day,
            next_day=(completed_day + 1) if not is_final_day else None,
            is_final_day=is_final_day,
            current_cash=player.current_cash,
            num_shares=player.num_shares,
            dividend=player.dividend_per_share,
            dividend_per_share=player.dividend_per_share,
            dividend_cash=player.dividend_cash,
            cash_after_dividend=player.cash_after_dividend,
            snapshot_error=player.daybreak_snapshot_error,
            forecast_price_next_day=player.field_maybe_none("forecast_price_next_day"),
            forecast_confidence_next_day=player.field_maybe_none("forecast_confidence_next_day"),
        )

    @staticmethod
    def error_message(player: Player, values):
        if _is_last_round_of_market(player.round_number):
            return None
        price = values.get("forecast_price_next_day")
        confidence = values.get("forecast_confidence_next_day")
        if price is None:
            return "Please enter a point forecast for next day closing price."
        if float(price) < 0:
            return "Forecasted price must be non-negative."
        if confidence is None:
            return "Please rate your confidence."
        try:
            conf_val = int(confidence)
        except (TypeError, ValueError):
            return "Confidence must be an integer from 1 to 5."
        if conf_val < 1 or conf_val > 5:
            return "Confidence must be between 1 and 5."
        return None


class MarketTransition(Page):
    @staticmethod
    def is_displayed(player: Player):
        market_number = _market_number_for_round(player.round_number)
        return _is_last_round_of_market(player.round_number) and market_number < C.NUM_MARKETS

    @staticmethod
    def vars_for_template(player: Player):
        completed_market = _market_number_for_round(player.round_number)
        return dict(
            completed_market=completed_market,
            next_market=completed_market + 1,
            total_markets=C.NUM_MARKETS,
            days_per_market=C.DAYS_PER_MARKET,
        )


class Results(Page):
    @staticmethod
    def is_displayed(player: Player):
        return _is_last_round_of_market(player.round_number)

    @staticmethod
    def vars_for_template(player: Player):
        _copy_market_start_trading_state(player.group)
        final_cash = _as_float(player.field_maybe_none("cash_after_dividend"), 0.0)
        total_shares = _as_float(player.field_maybe_none("num_shares"), 0.0)
        available_shares = total_shares
        reserved_shares = 0.0
        snapshot_error = ""

        trader_id = str(player.trader_uuid or "").strip()
        if trader_id and player.group.trading_api_base:
            try:
                snapshot = _fetch_trader_info(player.group, trader_id)
                final_cash = _as_float(snapshot.get("cash", final_cash), final_cash)
                total_shares = _as_float(snapshot.get("shares", total_shares), total_shares)
                available_shares = _as_float(snapshot.get("available_shares", available_shares), available_shares)
                reserved_shares = _as_float(snapshot.get("reserved_shares", reserved_shares), reserved_shares)
            except Exception as exc:
                snapshot_error = str(exc)

        initial_cash = _as_float(player.assigned_initial_cash, C.DEFAULT_INITIAL_CASH)
        delta_cash = final_cash - initial_cash

        return dict(
            market_number=_market_number_for_round(player.round_number),
            total_days=C.DAYS_PER_MARKET,
            final_cash=final_cash,
            initial_cash=initial_cash,
            delta_cash=delta_cash,
            final_total_shares=total_shares,
            final_available_shares=available_shares,
            final_reserved_shares=reserved_shares,
            snapshot_error=snapshot_error,
        )


page_sequence = [
    SyncTradingSession,
    ResumeTradingSession,
    InitFailed,
    TradePage,
    PauseTradingSession,
    DayBreak,
    MarketTransition,
]

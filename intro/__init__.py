from otree.api import *
from otree.api import Page as oTreePage
import ast
import json
from pprint import pprint
import logging
import os
from pathlib import Path
from sqlalchemy import Integer, BigInteger, SmallInteger, String, Text, Boolean, Float, Numeric
from sqlalchemy.inspection import inspect
import itertools
from user_agents import parse
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

doc = """
Introductory, consent, and round-1-only pages.
"""

SURVEY_PATH = Path(__file__).resolve().parent.parent / "data" / "comprehension_questions.yml"


def _format_placeholders(obj, context):
    if isinstance(obj, dict):
        return {k: _format_placeholders(v, context) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_format_placeholders(v, context) for v in obj]
    if isinstance(obj, str):
        try:
            return obj.format_map(context)
        except Exception:
            return obj
    return obj


COMPREHENSION_ANSWER_KEY = {"q1": "b", "q2": "d", "q3": "b", "q4": "c", "q5": "d"}


def _as_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _as_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _format_number(value):
    value = _as_float(value, 0.0)
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _as_number_list(raw, fallback):
    if isinstance(raw, (list, tuple)):
        parsed = []
        for item in raw:
            try:
                parsed.append(float(item))
            except (TypeError, ValueError):
                continue
        return parsed or list(fallback)

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return list(fallback)
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed_obj = parser(text)
            except Exception:
                continue
            if isinstance(parsed_obj, (list, tuple)):
                return _as_number_list(parsed_obj, fallback)
        parts = [x.strip() for x in text.split(",") if x.strip()]
        parsed = []
        for part in parts:
            try:
                parsed.append(float(part))
            except (TypeError, ValueError):
                continue
        return parsed or list(fallback)

    return list(fallback)


def _parse_endowment_options(raw_value, fallback):
    if not isinstance(raw_value, (list, tuple)):
        return list(fallback)
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
    return parsed or list(fallback)


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


def _forecast_days(n_days):
    total_days = max(1, _as_int(n_days, C.DEFAULT_DAYS_PER_MARKET))
    if total_days <= 3:
        return [1]
    return list(range(3, total_days, 3))


def _forecast_schedule_text(n_days):
    total_days = max(1, _as_int(n_days, C.DEFAULT_DAYS_PER_MARKET))
    if total_days == 1:
        return "This market has one period, so no forecast is collected."
    forecast_days = _forecast_days(total_days)
    period_label = "period" if len(forecast_days) == 1 else "periods"
    return f"You submit forecasts only after {period_label} {_natural_join(forecast_days)}."


def _experiment_params(player: "Player"):
    cfg = player.session.config
    num_markets = max(1, _as_int(cfg.get("num_markets", C.DEFAULT_NUM_MARKETS), C.DEFAULT_NUM_MARKETS))
    num_human_traders = max(1, _as_int(cfg.get("players_per_group", C.DEFAULT_GROUP_SIZE), C.DEFAULT_GROUP_SIZE))
    other_human_traders = max(0, num_human_traders - 1)
    days_per_market = max(
        1,
        _as_int(
            cfg.get("num_days", cfg.get("days_per_market", C.DEFAULT_DAYS_PER_MARKET)),
            C.DEFAULT_DAYS_PER_MARKET,
        ),
    )
    day_duration_minutes = max(
        1,
        _as_int(cfg.get("trading_day_duration", C.DEFAULT_TRADING_DAY_DURATION), C.DEFAULT_TRADING_DAY_DURATION),
    )
    market_total_minutes = days_per_market * day_duration_minutes
    forecast_bonus_amount = _as_float(
        cfg.get("forecast_bonus_amount", C.DEFAULT_FORECAST_BONUS_AMOUNT),
        C.DEFAULT_FORECAST_BONUS_AMOUNT,
    )
    forecast_bonus_threshold_pct = _as_float(
        cfg.get("forecast_bonus_threshold_pct", C.DEFAULT_FORECAST_BONUS_THRESHOLD_PCT),
        C.DEFAULT_FORECAST_BONUS_THRESHOLD_PCT,
    )
    forecast_bonus_wrong_pct = forecast_bonus_threshold_pct - 4.0
    if abs(forecast_bonus_wrong_pct - forecast_bonus_threshold_pct) < 1e-9:
        forecast_bonus_wrong_pct = forecast_bonus_threshold_pct + 1.0
    dividend_values = _as_number_list(
        cfg.get("dividend_values", cfg.get("dividends", C.DEFAULT_DIVIDEND_VALUES)),
        C.DEFAULT_DIVIDEND_VALUES,
    )
    unique_dividend_values = sorted(set(dividend_values))
    if len(unique_dividend_values) >= 4:
        quiz_dividend_values = unique_dividend_values[:4]
    else:
        quiz_dividend_values = list(C.DEFAULT_DIVIDEND_VALUES)
    expected_dividend = sum(quiz_dividend_values) / max(1, len(quiz_dividend_values))
    fundamental_value_start = expected_dividend * days_per_market
    fundamental_value_last = expected_dividend
    group_composition = str(player.participant.vars.get("group_composition", "") or "").strip().lower()
    has_algorithmic_traders = group_composition == "hybrid"
    endowment_options = _parse_endowment_options(
        cfg.get("human_trader_endowments"),
        C.DEFAULT_HUMAN_TRADER_ENDOWMENTS,
    )
    exchange_rate = _as_float(cfg.get("real_world_currency_per_point", 1), 1)
    quiz_bonus_per_correct = _as_float(cfg.get("fee_per_correct_answer", 1), 1)

    example_dividend = quiz_dividend_values[-1]
    return dict(
        num_human_traders=num_human_traders,
        other_human_traders=other_human_traders,
        has_algorithmic_traders=has_algorithmic_traders,
        num_markets=num_markets,
        num_days=days_per_market,
        total_periods=num_markets * days_per_market,
        trading_day_duration=day_duration_minutes,
        market_total_minutes=market_total_minutes,
        endowment_options_text=_format_endowment_options_text(endowment_options),
        expected_dividend=_money(expected_dividend),
        fundamental_value_start=_money(fundamental_value_start),
        fundamental_value_step=_money(expected_dividend),
        fundamental_value_last=_money(fundamental_value_last),
        forecast_bonus_amount=_format_number(forecast_bonus_amount),
        forecast_bonus_threshold_pct=_format_number(forecast_bonus_threshold_pct),
        forecast_bonus_wrong_pct=_format_number(forecast_bonus_wrong_pct),
        dividend_values_text=_money_series_text(quiz_dividend_values),
        dividend_constant_wrong=_money(quiz_dividend_values[min(2, len(quiz_dividend_values) - 1)]),
        example_period_current=3,
        example_period_next=4,
        example_dividend=_money(example_dividend),
        payoff_period=days_per_market,
        exchange_rate_text=_format_number(exchange_rate),
        quiz_bonus_per_correct_text=_format_number(quiz_bonus_per_correct),
        forecast_schedule_text=_forecast_schedule_text(days_per_market),
    )


def load_comprehension_survey(context):
    if yaml is None:
        raise RuntimeError("PyYAML is required to load comprehension survey.")
    if not SURVEY_PATH.exists():
        raise FileNotFoundError(f"Comprehension survey file not found at {SURVEY_PATH}")
    with SURVEY_PATH.open() as f:
        data = yaml.safe_load(f) or {}
    return _format_placeholders(data, context)


def process_survey_data(player, survey_results):
    mapper = inspect(player.__class__)

    for key, value in survey_results.items():
        logger.info(f'Processing {key}: {value}')
        try:
            if key not in mapper.columns:
                logger.warning(f'No such field: {key}')
                continue

            column = mapper.columns[key]
            column_type = column.type

            if isinstance(column_type, (Integer, BigInteger, SmallInteger)):
                if isinstance(value, int):
                    converted_value = value
                elif isinstance(value, str) and value.isdigit():
                    converted_value = int(value)
                else:
                    converted_value = int(value)
            elif isinstance(column_type, (String, Text)):
                converted_value = str(value)
            elif isinstance(column_type, Boolean):
                if isinstance(value, bool):
                    converted_value = value
                elif isinstance(value, str):
                    converted_value = value.lower() in ['true', '1', 'yes']
                else:
                    converted_value = bool(value)
            elif isinstance(column_type, (Float, Numeric)):
                converted_value = float(value)
            else:
                converted_value = value

            setattr(player, key, converted_value)
            logger.info(f'Successfully set {key} to {converted_value}')

        except ValueError as ve:
            logger.error(f'Value error for field "{key}": {value} - {ve}')
        except Exception as e:
            logger.error(f'Error setting field "{key}": {e}')


class Page(oTreePage):
    instructions = False

    def get_context_data(self, **context):
        NUM_SURVEY_PAGES = 36
        player = self.player
        exp_params= _experiment_params(player)  # ensure params are computed for template context
        app_name = self.__module__.split('.')[0]
        page_name = self.__class__.__name__
        if page_name != 'PostSurvey' and app_name == 'post_exp':
            index_in_pages = self._index_in_pages + NUM_SURVEY_PAGES
        else:
            index_in_pages = self._index_in_pages
        r = super().get_context_data(**context)

        if 'post_exp' in self.session.config.get('app_sequence'):
            max_pages = NUM_SURVEY_PAGES + self.participant._max_page_index
        else:
            max_pages = self.participant._max_page_index

        r['maxpages'] = max_pages
        r['page_index'] = self._index_in_pages
        r['progress'] = f'{int(index_in_pages / max_pages * 100):d}'
        r['instructions'] = self.instructions
        exchange_rate = self.session.config.get('real_world_currency_per_point', 1)
        pay_for_correct = self.session.config.get('fee_per_correct_answer', 1)
        r['pay_for_correct_real'] = pay_for_correct * exchange_rate
        r['fee_amount'] = self.session.config.get('fee_amount', 4)
        r['endowment'] = self.session.config.get('endowment', 100)
        r['belief_bonus_amount'] = self.session.config.get('belief_bonus_amount', 1)
        r['forecast_bonus_amount'] = self.session.config.get('forecast_bonus_amount', 1)
        r['condition'] = self.participant.vars.get('condition', 'gh')
        r.update(exp_params)
        return r


class SurveyJSPage(Page):
    def post(self):
        if self.participant.is_browser_bot:
            return super().post()

        try:
            survey_results = json.loads(self._form_data.get('surveyResults') or '{}')
            pprint(survey_results)
            process_survey_data(self.player, survey_results)
            self.player.attention_check_passed = all(
                str(survey_results.get(key, '')).strip() == expected
                for key, expected in COMPREHENSION_ANSWER_KEY.items()
            )
        except json.JSONDecodeError as e:
            logger.error(f'Invalid JSON data: {e}')
        except Exception as e:
            logger.error(f'Unexpected error: {e}')

        return super().post()


class C(BaseConstants):
    NAME_IN_URL = 'intro'
    PLAYERS_PER_GROUP = max(2, int(os.getenv("PLAYERS_PER_GROUP", 2)))
    NUM_ROUNDS = 1
    TREATMENTS = ("gh", "nh", "gm", "nm")
    WEIGHTED_TREATMENT_SEQUENCE = ("gh", "gh", "nh", "nh", "gm", "nm")
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
    DEFAULT_NUM_MARKETS = max(1, int(os.getenv("NUM_MARKETS", 2)))
    DEFAULT_DAYS_PER_MARKET = max(1, int(os.getenv("DAYS_PER_MARKET", 2)))
    DEFAULT_TRADING_DAY_DURATION = 1
    DEFAULT_FORECAST_BONUS_AMOUNT = 1
    DEFAULT_FORECAST_BONUS_THRESHOLD_PCT = 1
    DEFAULT_HYBRID_NOISE_TRADERS = 1
    DEFAULT_GROUP_SIZE = PLAYERS_PER_GROUP
    DEFAULT_DIVIDEND_VALUES = (0, 4, 8, 20)
    DEFAULT_HUMAN_TRADER_ENDOWMENTS = (
        (2600.0, 20),
        (3800.0, 10),
    )


class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    treatment = models.StringField(initial="gh")
    market_design = models.StringField(initial="gamified")
    group_composition = models.StringField(initial="human_only")


def _parse_treatments(raw_value):
    if raw_value is None:
        return list(C.WEIGHTED_TREATMENT_SEQUENCE)
    if isinstance(raw_value, str):
        candidate_values = [x.strip().lower() for x in raw_value.split(",")]
    elif isinstance(raw_value, (list, tuple)):
        candidate_values = [str(x).strip().lower() for x in raw_value]
    else:
        return list(C.WEIGHTED_TREATMENT_SEQUENCE)
    filtered = [x for x in candidate_values if x in C.TREATMENTS]
    return filtered or list(C.WEIGHTED_TREATMENT_SEQUENCE)


def _set_group_treatment(group: Group, treatment: str):
    treatment_value = str(treatment or "").strip().lower()
    if treatment_value not in C.TREATMENTS:
        treatment_value = C.WEIGHTED_TREATMENT_SEQUENCE[0]
    group.treatment = treatment_value
    group.market_design = C.TREATMENT_MARKET_DESIGN[treatment_value]
    group.group_composition = C.TREATMENT_GROUP_COMPOSITION[treatment_value]


def creating_session(subsession):
    if subsession.round_number != 1:
        return
    treatment_cycle = itertools.cycle(_parse_treatments(subsession.session.config.get("treatments")))
    for group in subsession.get_groups():
        _set_group_treatment(group, next(treatment_cycle))
        for player in group.get_players():
            player.condition = group.treatment
            player.participant.vars["condition"] = group.treatment
            player.participant.vars["treatment"] = group.treatment
            player.participant.vars["market_design"] = group.market_design
            player.participant.vars["group_composition"] = group.group_composition


class Player(BasePlayer):
    consent = models.BooleanField()
    condition = models.StringField()

    # BLOCK OF USER AGENT INFO
    user_agent_browser = models.StringField()
    user_agent_browser_version = models.StringField()
    user_agent_os = models.StringField()
    user_agent_os_version = models.StringField()
    user_agent_device = models.StringField()
    user_agent_is_bot = models.BooleanField()
    user_agent_is_mobile = models.BooleanField()
    user_agent_is_tablet = models.BooleanField()
    user_agent_is_pc = models.BooleanField()

    # comprehension quiz fields
    attention_check = models.StringField(blank=True)
    attention_check_passed = models.BooleanField(blank=True)
    q1 = models.StringField(
        label="At the end of each period, the asset pays a dividend. Which statement is correct?",
        widget=widgets.RadioSelect,
        choices=[
            ('a', "The dividend is always E$8."),
            ('b', "The dividend is equally likely to be E$0, E$4, E$8, or E$20."),
            ('c', "The dividend depends on the asset's trading price."),
            ('d', "The dividend is determined by your trading activity."),
        ],
    )
    q2 = models.StringField(
        label="At the end of each period, you forecast the next-period stock price. When do you earn a bonus?",
        widget=widgets.RadioSelect,
        choices=[
            ('a', "If your forecast is within 5% of the realized price."),
            ('b', "If your forecast exactly equals the realized price."),
            ('c', "You earn E$100 for every forecast submitted."),
            ('d', "If your forecast is within 1% of the realized price."),
        ],
    )
    q3 = models.StringField(
        label="Suppose the dividend in period 3 is E$20. How does this affect fundamental value in period 4?",
        widget=widgets.RadioSelect,
        choices=[
            ('a', "The fundamental value increases by E$20."),
            ('b', "The fundamental value is unaffected by the realized dividend."),
            ('c', "The fundamental value decreases by E$20."),
            ('d', "The fundamental value equals E$20."),
        ],
    )
    q4 = models.StringField(
        label="Which best describes how your final payoff is determined?",
        widget=widgets.RadioSelect,
        choices=[
            ('a', "Cash at the end of period 15, including period-15 dividends."),
            ('b', "The number of shares held at the end of period 15."),
            ('c', "Cash including period-15 dividends plus any forecast bonuses earned."),
            ('d', "Cash at the start of period 15 plus any forecast bonuses earned."),
        ],
    )
    q5 = models.StringField(
        label="Which actions are not permitted in this market?",
        widget=widgets.RadioSelect,
        choices=[
            ('a', "Selling shares you do not own (short selling)."),
            ('b', "Borrowing cash to purchase additional shares."),
            ('c', "Buying shares only if you have sufficient cash."),
            ('d', "Both (a) and (b)."),
        ],
    )

    # Self-assessment field
    self_assesment = models.IntegerField(
        label='On a scale from zero to ten, where zero is not at all knowledgeable about personal finance and ten is very knowledgeable about personal finance, what number would you be on the scale?',
        choices=range(11),
        widget=widgets.RadioSelect
    )

    # WTP Certainty field
class Consent(Page):
    form_model = 'player'
    form_fields = ['consent']

    def get(self, *args, **kwargs):
        user_agent_string = self.request.headers.get('User-Agent')
        user_agent = parse(user_agent_string)

        res = {
            'browser': user_agent.browser.family,
            'browser_version': user_agent.browser.version_string,
            'os': user_agent.os.family,
            'os_version': user_agent.os.version_string,
            'device': user_agent.device.family,
            'is_mobile': user_agent.is_mobile,
            'is_tablet': user_agent.is_tablet,
            'is_pc': user_agent.is_pc,
            'is_bot': user_agent.is_bot
        }
        for k, v in res.items():
            try:
                self.player.__setattr__(f'user_agent_{k}', v)
            except AttributeError:
                logger.warning(f"{f'user_agent_{k}'} not found in player model")
        return super().get(*args, **kwargs)


class Instructions(Page):
    @staticmethod
    def vars_for_template(player: Player):
        return _experiment_params(player)


class comprehensionQuestions(SurveyJSPage):
    instructions = True
    @staticmethod
    def js_vars(player: Player):
        survey = load_comprehension_survey(_experiment_params(player))
        return dict(
            survey_json=survey
        )


class selfAssessment(Page):
    form_model = 'player'
    form_fields = ['self_assesment']


page_sequence = [
    Consent,
    Instructions,
    comprehensionQuestions,
    selfAssessment,
]

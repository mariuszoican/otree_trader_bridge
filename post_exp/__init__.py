from otree.api import *
from otree.api import Page as oTreePage
import csv
import json
from pprint import pprint
import random
import logging
from pathlib import Path
from functools import lru_cache
from sqlalchemy import (
    Integer,
    BigInteger,
    SmallInteger,
    String,
    Text,
    Boolean,
    Float,
    Numeric,
)
from sqlalchemy.inspection import inspect
from starlette.responses import RedirectResponse

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
countries_path = DATA_DIR / "countries.csv"
quiz_answers_path = DATA_DIR / "fin_quiz_answers.csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

doc = """
Post-experimental pages including literacy, demographics, and payout.
"""


@lru_cache(maxsize=1)
def load_countries():
    if not countries_path.exists():
        raise FileNotFoundError(f"Countries CSV not found at {countries_path}")
    countries = []
    with countries_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = str(row.get("Country", "")).strip()
            if country:
                countries.append(country)
    return countries


@lru_cache(maxsize=1)
def load_quiz_answer_key():
    if not quiz_answers_path.exists():
        raise FileNotFoundError(f"Quiz answers CSV not found at {quiz_answers_path}")
    answer_key = {}
    with quiz_answers_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            variable_name = str(row.get("variable_name", "")).strip()
            correct_answer = str(row.get("correct_answer", "")).strip()
            if variable_name:
                answer_key[variable_name] = correct_answer
    return answer_key


def resolve_trade_payoff_from_selected_market(participant, payable_market, num_days):
    stored_market_payoffs = participant.vars.get("market_cash_after_dividend") or {}
    for key in (payable_market, str(payable_market)):
        if key not in stored_market_payoffs:
            continue
        try:
            return cu(stored_market_payoffs[key])
        except Exception:
            return None

    target_round = max(1, int(payable_market)) * max(1, int(num_days))
    try:
        all_players = participant.get_players()
    except Exception:
        return None

    for candidate in all_players:
        if not hasattr(candidate, "cash_after_dividend"):
            continue
        if int(getattr(candidate, "round_number", 0) or 0) != target_round:
            continue
        final_cash = getattr(candidate, "cash_after_dividend", None)
        if final_cash in (None, "") and hasattr(candidate, "field_maybe_none"):
            final_cash = candidate.field_maybe_none("cash_after_dividend")
        if final_cash in (None, ""):
            continue
        try:
            return cu(final_cash)
        except Exception:
            return None
    return None


def assign_total_payoff(player):
    trade_payoff = player.participant.vars.get(
        "payoff_for_trade",
        player.payoff_for_trade or cu(0),
    )
    quiz_payoff = player.participant.vars.get(
        "payoff_for_quiz",
        player.payoff_for_quiz or cu(0),
    )
    bonus_total = player.participant.vars.get("cumulative_bonuses", cu(0))
    total = trade_payoff + quiz_payoff + bonus_total
    player.payoff_for_trade = trade_payoff
    player.payoff = total
    player.participant.payoff = total
    player.participant.vars["total_bonus"] = total
    return dict(
        trade_payoff=trade_payoff,
        quiz_payoff=quiz_payoff,
        bonus_total=bonus_total,
        total=total,
    )


def process_survey_data(player, survey_results):
    mapper = inspect(player.__class__)

    for key, value in survey_results.items():
        logger.info(f"Processing {key}: {value}")
        try:
            if key not in mapper.columns:
                logger.warning(f"No such field: {key}")
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
                    converted_value = value.lower() in ["true", "1", "yes"]
                else:
                    converted_value = bool(value)
            elif isinstance(column_type, (Float, Numeric)):
                converted_value = float(value)
            else:
                converted_value = value

            setattr(player, key, converted_value)
            logger.info(f"Successfully set {key} to {converted_value}")

        except ValueError as ve:
            logger.error(f'Value error for field "{key}": {value} - {ve}')
        except Exception as e:
            logger.error(f'Error setting field "{key}": {e}')


class Page(oTreePage):
    instructions = False

    def get_context_data(self, **context):
        NUM_SURVEY_PAGES = 36
        app_name = self.__module__.split(".")[0]
        page_name = self.__class__.__name__
        if page_name != "PostSurvey" and app_name == "post_exp":
            index_in_pages = self._index_in_pages + NUM_SURVEY_PAGES
        else:
            index_in_pages = self._index_in_pages
        r = super().get_context_data(**context)

        if "post_exp" in self.session.config.get("app_sequence"):
            max_pages = NUM_SURVEY_PAGES + self.participant._max_page_index
        else:
            max_pages = self.participant._max_page_index

        r["maxpages"] = max_pages
        r["page_index"] = self._index_in_pages
        r["progress"] = f"{int(index_in_pages / max_pages * 100):d}"
        r["instructions"] = self.instructions
        exchange_rate = self.session.config.get("real_world_currency_per_point", 1)
        pay_for_correct = self.session.config.get("fee_per_correct_answer", 1)
        r["pay_for_correct_real"] = pay_for_correct * exchange_rate
        r["fee_amount"] = self.session.config.get("fee_amount", 4)
        r["endowment"] = self.session.config.get("endowment", 100)
        r["belief_bonus_amount"] = self.session.config.get("belief_bonus_amount", 1)
        r["forecast_bonus_amount"] = self.session.config.get("forecast_bonus_amount", 1)
        r["condition"] = self.participant.vars.get("condition", "control")
        return r


class SurveyJSPage(Page):
    def post(self):
        print(self.participant.is_browser_bot)
        try:
            survey_results = json.loads(self._form_data.get("surveyResults"))
            pprint(survey_results)
            print('*'* 200)
            process_survey_data(self.player, survey_results)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON data: {e}"  )
            pprint(self.__dict__, indent=4)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return super().post()


def creating_session(subsession):
    num_markets = max(1, int(subsession.session.config.get("num_markets", 2) or 2))
    num_days = max(1, int(subsession.session.config.get("num_days", 1) or 1))
    for p in subsession.get_players():
        payable_market = p.participant.vars.get("payable_market")
        if payable_market is None:
            legacy_round = p.participant.vars.get("payable_round")
            if legacy_round is not None:
                payable_market = ((int(legacy_round) - 1) // num_days) + 1
            else:
                payable_market = random.randint(1, num_markets)
        payable_market = max(1, min(int(payable_market), num_markets))
        p.payable_market = payable_market
        stored_trade_payoff = p.participant.vars.get("payoff_for_trade")
        if stored_trade_payoff is not None:
            trade_payoff = cu(stored_trade_payoff)
        else:
            trade_payoff = resolve_trade_payoff_from_selected_market(
                p.participant,
                payable_market=payable_market,
                num_days=num_days,
            )
            if trade_payoff is None:
                trade_payoff = cu(0)
        p.payoff_for_trade = trade_payoff
        bonus_total = p.participant.vars.get("cumulative_bonuses", cu(0))
        p.participant.vars.setdefault("cumulative_bonuses", bonus_total)
        p.participant.vars["payable_market"] = payable_market
        p.participant.vars["payoff_for_trade"] = trade_payoff


class C(BaseConstants):
    COUNTRIES = load_countries()
    NAME_IN_URL = "post_exp"
    PLAYERS_PER_GROUP = None
    NUM_ROUNDS = 1


class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    pass


class Player(BasePlayer):
    payable_market = models.IntegerField()
    payoff_for_trade = models.CurrencyField()

    # Literacy Quiz Fields
    savings_interest = models.StringField()
    inflation_effect = models.StringField()
    bid_price_definition = models.StringField()
    mortgage_comparison = models.StringField()
    asset_fluctuations = models.StringField()
    investment_risk = models.StringField()
    long_term_returns = models.StringField()
    stock_mutual_fund_loss = models.StringField()
    bid_ask_spread = models.StringField()
    mutual_fund_statements = models.StringField()
    bond_investment = models.StringField()
    credit_card_payment = models.StringField()
    num_quiz_questions = models.IntegerField()
    num_correct_answers = models.IntegerField()
    payoff_for_quiz = models.CurrencyField()
    payoff_for_quiz_expected = models.CurrencyField()

    #     demographic fields
    gender = models.StringField()
    age = models.IntegerField()

    nationality = models.StringField()
    education = models.StringField()
    study_major = models.StringField()
    course_financial = models.BooleanField(
        label="Did you take any course focused on financial markets"
    )
    experiment_before = models.BooleanField(
        label="Have you been taken part in an experiment before?"
    )
    trading_experience = models.BooleanField(
        label="Do you have any trading experience?"
    )
    online_trading_experience = models.BooleanField(
        label="Do you use mobile trading apps?"
    )
    trading_frequency = models.StringField(label="How often do you trade online?")
    portfolio_frequency = models.StringField(
        label="How often do you check the value of your portfolio?"
    )
    asset_class = models.StringField(label="Which asset class do you trade the most?")
    use_leverage = models.StringField(
        label="Do you use leverage (e.g., trading on margin)?"
    )

    # Feedback questions
    purpose = models.LongStringField(
        label="What do you think is the purpose of this study?", default=""
    )
    difficulty = models.LongStringField(
        label="Did you encounter any difficulty throughout the experiment?", default=""
    )

    # Pilot feedback questions
    pilot_difficulty = models.IntegerField()
    pilot_instruction_clarity = models.IntegerField()
    pilot_unclear_open = models.LongStringField()
    pilot_payoff_understanding = models.IntegerField()
    pilot_interface_clarity = models.IntegerField()
    pilot_tech_issues = models.StringField()
    pilot_tech_issues_desc = models.LongStringField()
    pilot_flow_unsure_steps = models.LongStringField()
    pilot_one_change = models.LongStringField()
    pilot_surprises_mismatch = models.LongStringField()
    pilot_missing_info = models.LongStringField()
    pilot_confusing_wording = models.LongStringField()


class literacyQuiz(SurveyJSPage):
    instructions = False

    @staticmethod
    def is_displayed(player: Player):
        return player.round_number == C.NUM_ROUNDS

    @staticmethod
    def before_next_page(player, timeout_happened):
        correct_answers = load_quiz_answer_key()
        player.num_quiz_questions = len(correct_answers)
        num_correct_answers = 0
        if player.participant.is_browser_bot:
            for question, correct_answer in correct_answers.items():
                if random.random() < 0.5:
                    setattr(player, question, correct_answer)
                else:
                    wrong_answer = f"not_{correct_answer}"
                    setattr(player, question, wrong_answer)

        for question, correct_answer in correct_answers.items():
            try:
                player_answer = getattr(player, question, None)
            except TypeError:
                player_answer = None
            if player_answer == correct_answer:
                num_correct_answers += 1

        player.num_correct_answers = num_correct_answers
        fee_per_correct = player.session.config.get("fee_per_correct_answer", 1)
        expected_quiz_payoff = player.num_quiz_questions * fee_per_correct
        player.payoff_for_quiz_expected = expected_quiz_payoff
        player.participant.vars["quiz_payoff_expected"] = expected_quiz_payoff
        player.payoff_for_quiz = num_correct_answers * fee_per_correct
        player.participant.vars["payoff_for_quiz"] = player.payoff_for_quiz
        player.payoff = player.payoff_for_quiz


class Demographics(SurveyJSPage):
    @staticmethod
    def js_vars(player: Player):
        return dict(countries=C.COUNTRIES)

    @staticmethod
    def is_displayed(player: Player):
        return player.round_number == C.NUM_ROUNDS


class Payoff(Page):
    @staticmethod
    def is_displayed(player: Player):
        return player.round_number == C.NUM_ROUNDS

    @staticmethod
    def vars_for_template(player: Player):
        payoff_parts = assign_total_payoff(player)
        trade_payoff = payoff_parts["trade_payoff"]
        quiz_payoff = payoff_parts["quiz_payoff"]
        bonus_total = payoff_parts["bonus_total"]
        total_points = payoff_parts["total"]
        fee_per_correct = player.session.config.get("fee_per_correct_answer", 1)
        exchange_rate = player.session.config.get("real_world_currency_per_point", 1)
        participation_fee = player.session.config.get("participation_fee", 0)
        total_markets = max(1, int(player.session.config.get("num_markets", 2) or 2))
        cash_bonus = total_points * exchange_rate
        total_real = cash_bonus + participation_fee
        return dict(
            trade_payoff=trade_payoff,
            quiz_payoff=quiz_payoff,
            total_points=total_points,
            cash_bonus=cash_bonus,
            total_real=total_real,
            fee_per_correct=fee_per_correct,
            exchange_rate=exchange_rate,
            participation_fee=participation_fee,
            payable_market=player.payable_market,
            total_markets=total_markets,
            bonus_total=bonus_total,
        )

    @staticmethod
    def before_next_page(player: Player, timeout_happened):
        payoff_parts = assign_total_payoff(player)
        trade_payoff = payoff_parts["trade_payoff"]
        quiz_payoff = payoff_parts["quiz_payoff"]
        bonus_total = payoff_parts["bonus_total"]
        print(
            f"INNER COMPONENTS: trade {trade_payoff}, quiz {quiz_payoff}, bonus {bonus_total}"
        )


class PilotFeedback(SurveyJSPage):
    @staticmethod
    def is_displayed(player: Player):
        return player.round_number == C.NUM_ROUNDS


class FinalForProlific(Page):
    @staticmethod
    def is_displayed(player: Player):
        prol = player.session.config.get("for_prolific", False)
        return player.round_number == C.NUM_ROUNDS and prol

    def get(self):
        prol = self.player.session.config.get("for_prolific", False)
        if not prol:
            return super().get()
        base_return_url = self.session.config.get(
            "prolific_base_return_url", "https://cnn.com"
        )

        if not self.participant.label:
            ending = self.session.config.get("prolific_no_id_code", "NO_ID")
        else:
            ending = self.session.config.get("prolific_return_code", "CW6532UV")
        full_return_url = f"{base_return_url}{ending}"
        return RedirectResponse(full_return_url)


page_sequence = [
    literacyQuiz,
    Demographics,
    Payoff,
    PilotFeedback,
    FinalForProlific,
]

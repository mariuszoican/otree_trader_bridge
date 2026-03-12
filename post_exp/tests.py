import json
import csv
import random
from pathlib import Path

from otree.api import Currency as cu, Submission, Bot
from . import *


QUIZ_ANSWERS = Path(__file__).resolve().parent.parent / "data" / "fin_quiz_answers.csv"


def _bot_rng(bot, salt=""):
    return random.Random(f"{bot.session.code}:{bot.participant.code}:{bot.round_number}:{salt}")


def load_quiz_answers():
    answers = {}
    with QUIZ_ANSWERS.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            answers[row["variable_name"]] = row["correct_answer"]
    return answers


class PlayerBot(Bot):
    def play_round(self):
        rng = _bot_rng(self, "post_exp")
        # Literacy quiz with correct answers to exercise payoff calc
        quiz_answers = load_quiz_answers()
        randomized_answers = quiz_answers
        yield Submission(
            literacyQuiz,
            dict(surveyResults=json.dumps(randomized_answers), ),
            check_html=False,
        )

        # Demographics (randomized survey payload)
        demo_payload = {
            "gender": rng.choice(["Male", "Female", "Other"]),
            "age": rng.randint(18, 70),
            "nationality": rng.choice(["USA", "Canada", "Other"]),
            "education": rng.choice(["High school", "College", "Graduate"]),
            "study_major": rng.choice(["Economics", "Finance", "Other"]),
            "course_financial": rng.choice([True, False]),
            "experiment_before": rng.choice([True, False]),
            "trading_experience": rng.choice([True, False]),
            "online_trading_experience": rng.choice([True, False]),
            "trading_frequency": rng.choice(["Daily", "Weekly", "Monthly"]),
            "portfolio_frequency": rng.choice(["Daily", "Weekly", "Monthly"]),
            "asset_class": rng.choice(["Stocks", "Bonds", "Crypto"]),
            "use_leverage": rng.choice([True, False]),
            "purpose": f"Test purpose {rng.randint(1, 999)}",
            "difficulty": rng.choice(["No issues", "Mostly clear", "A bit confusing"]),
        }
        yield Submission(
            Demographics,
            dict(surveyResults=json.dumps(demo_payload)),
            check_html=False,
        )

        # Payoff page: verify aggregation
        yield Payoff
        fee_per_correct = self.session.config.get('fee_per_correct_answer', 1)

        trade_payoff = self.participant.vars.get('payoff_for_trade', cu(0))
        cumulative_bonuses = self.participant.vars.get('cumulative_bonuses', cu(0))
        quiz_payoff_expected = cu(len(quiz_answers) * fee_per_correct)

        total_expected = trade_payoff + cumulative_bonuses + quiz_payoff_expected
        print(f'components: trade {trade_payoff}, bonuses {cumulative_bonuses}, quiz {quiz_payoff_expected}')
        print("Expected total payoff:", total_expected)
        print("Participant total bonus:", self.participant.vars['total_bonus'])

        assert self.player.payoff_for_quiz_expected == quiz_payoff_expected
        assert self.participant.vars["quiz_payoff_expected"] == quiz_payoff_expected
        assert self.participant.vars['total_bonus'] == total_expected
        assert self.player.payoff == total_expected
        pilot_payload = {
            "pilot_difficulty": rng.randint(1, 7),
            "pilot_instruction_clarity": rng.randint(1, 7),
            "pilot_unclear_open": rng.choice(["Nothing was unclear.", "Some wording could be shorter."]),
            "pilot_payoff_understanding": rng.randint(1, 7),
            "pilot_interface_clarity": rng.randint(1, 7),
            "pilot_tech_issues": rng.choice(["No", "Minor lag"]),
            "pilot_tech_issues_desc": rng.choice(["No issues.", "Minor lag once."]),
            "pilot_flow_unsure_steps": rng.choice(["I was never unsure.", "One transition felt abrupt."]),
            "pilot_one_change": rng.choice(["No changes.", "Add one more example."]),
            "pilot_surprises_mismatch": rng.choice(["Nothing differed.", "One instruction was unexpected."]),
            "pilot_missing_info": rng.choice(["No.", "Maybe one more payoff example."]),
            "pilot_confusing_wording": rng.choice(["No.", "One phrase could be clearer."]),
        }
        yield Submission(
            PilotFeedback,
            dict(surveyResults=json.dumps(pilot_payload)),
            check_html=False,
        )

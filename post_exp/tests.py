import json
import csv
import random
import unittest
from pathlib import Path
from types import SimpleNamespace

from otree.api import Currency as cu, Submission, Bot
from . import *


QUIZ_ANSWERS = Path(__file__).resolve().parent.parent / "data" / "fin_quiz_answers.csv"


def load_quiz_answers():
    answers = {}
    with QUIZ_ANSWERS.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            answers[row["variable_name"]] = row["correct_answer"]
    return answers


class PlayerBot(Bot):
    def play_round(self):
        # Literacy quiz with correct answers to exercise payoff calc
        quiz_answers = load_quiz_answers()
        randomized_answers = quiz_answers
        yield Submission(
            literacyQuiz,
            dict(surveyResults=json.dumps(randomized_answers), ),
            check_html=False,
        )

        yield LabContact, dict(ucid="U1234567", email="participant@example.com")

        # Demographics (randomized survey payload)
        demo_payload = {
            "gender": random.choice(["Male", "Female", "Other"]),
            "age": random.randint(18, 70),
            "nationality": random.choice(["USA", "Canada", "Other"]),
            "education": random.choice(["High school", "College", "Graduate"]),
            "study_major": random.choice(["Economics", "Finance", "Other"]),
            "course_financial": random.choice([True, False]),
            "experiment_before": random.choice([True, False]),
            "trading_experience": random.choice([True, False]),
            "online_trading_experience": random.choice([True, False]),
            "trading_frequency": random.choice(["Daily", "Weekly", "Monthly"]),
            "portfolio_frequency": random.choice(["Daily", "Weekly", "Monthly"]),
            "asset_class": random.choice(["Stocks", "Bonds", "Crypto"]),
            "use_leverage": random.choice([True, False]),
            "purpose": "Test purpose text",
            "difficulty": "No issues",
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
            "pilot_difficulty": random.randint(1, 7),
            "pilot_instruction_clarity": random.randint(1, 7),
            "pilot_unclear_open": "Nothing was unclear.",
            "pilot_payoff_understanding": random.randint(1, 7),
            "pilot_interface_clarity": random.randint(1, 7),
            "pilot_tech_issues": "No",
            "pilot_tech_issues_desc": "No issues.",
            "pilot_flow_unsure_steps": "I was never unsure.",
            "pilot_one_change": "No changes.",
            "pilot_surprises_mismatch": "Nothing differed.",
            "pilot_missing_info": "No.",
            "pilot_confusing_wording": "No.",
        }
        yield Submission(
            PilotFeedback,
            dict(surveyResults=json.dumps(pilot_payload)),
            check_html=False,
        )


class PostExpPageTests(unittest.TestCase):
    def test_lab_contact_defaults_to_visible(self):
        player = SimpleNamespace(round_number=1, session=SimpleNamespace(config={}))
        assert LabContact.is_displayed(player) is True

    def test_lab_contact_respects_toggle(self):
        player = SimpleNamespace(round_number=1, session=SimpleNamespace(config={"show_lab_contact_page": False}))
        assert LabContact.is_displayed(player) is False

    def test_lab_contact_accepts_blank_email(self):
        assert LabContact.error_message(None, {"email": ""}) is None

    def test_lab_contact_rejects_invalid_email(self):
        assert LabContact.error_message(None, {"email": "not-an-email"}) == "Please enter a valid e-mail address or leave it blank."

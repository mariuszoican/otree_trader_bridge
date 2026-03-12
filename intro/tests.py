import json
import random
from otree.api import Bot, Submission
from . import *


def _bot_rng(bot, salt=""):
    return random.Random(f"{bot.session.code}:{bot.participant.code}:{bot.round_number}:{salt}")


class PlayerBot(Bot):
    def play_round(self):
        # Consent
        yield Consent, dict(consent=True)

        # Instructions (no form)
        yield Instructions

        # Comprehension (SurveyJS; submit via hidden field)
        survey_payload = {
            "q1": "b",
            "q2": "d",
            "q3": "b",
            "q4": "c",
            "q5": "d",
        }
        yield Submission(
            comprehensionQuestions,
            dict(surveyResults=json.dumps(survey_payload)),
            check_html=False,
        )

        # Self assessment
        rng = _bot_rng(self, "self_assessment")
        yield selfAssessment, dict(self_assesment=rng.randint(1, 7))

import json
from otree.api import Bot, Submission
from . import *


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
            "q3": "a",
            "q4": "c",
            "q5": "d",
        }
        yield Submission(
            comprehensionQuestions,
            dict(
                surveyResults=json.dumps(survey_payload),
                cqAttemptCount="2",
                cqWrongFirstTry="true",
            ),
            check_html=False,
        )
        assert self.player.attention_check_passed is True
        assert self.player.cq_attempt_count == 2
        assert self.player.cq_wrong_first_try is True

        # Self assessment
        yield selfAssessment, dict(self_assesment=5)

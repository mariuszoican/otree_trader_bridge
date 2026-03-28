import json
import unittest
from types import SimpleNamespace

from otree.api import Bot, Submission
from . import *


class PlayerBot(Bot):
    def play_round(self):
        # Consent
        yield Consent, dict(consent=True)

        # Instructions (no form)
        yield Instructions

        yield InstructionsVideo

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


class IntroPageTests(unittest.TestCase):
    def test_instructions_video_defaults_to_visible(self):
        player = SimpleNamespace(session=SimpleNamespace(config={}))
        assert InstructionsVideo.is_displayed(player) is True

    def test_instructions_video_respects_toggle(self):
        player = SimpleNamespace(session=SimpleNamespace(config={"show_intro_video_page": False}))
        assert InstructionsVideo.is_displayed(player) is False

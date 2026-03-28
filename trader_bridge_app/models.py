from otree.api import BaseGroup, BasePlayer, BaseSubsession, models, widgets

from .constants import C


class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    trading_session_uuid = models.StringField(blank=True)
    trading_api_base = models.StringField(blank=True)
    trading_ws_base = models.StringField(blank=True)
    trading_init_error = models.LongStringField(blank=True)
    trading_day_duration_minutes = models.IntegerField(initial=C.DEFAULT_TRADING_DAY_DURATION)
    treatment = models.StringField(initial="gh")
    market_design = models.StringField(initial="gamified")
    group_composition = models.StringField(initial="human_only")
    noise_trader_draw = models.FloatField(initial=0)
    noise_trader_present = models.BooleanField(initial=False)
    num_days = models.IntegerField(initial=C.DEFAULT_NUM_DAYS)
    dividends_csv = models.LongStringField(blank=True)
    observed_last_transaction_price = models.FloatField(blank=True)
    closing_price = models.FloatField(blank=True)


class Player(BasePlayer):
    trader_uuid = models.StringField(blank=True)
    current_cash = models.FloatField(initial=0)
    num_shares = models.FloatField(initial=0)
    dividend_per_share = models.FloatField(initial=0)
    dividend_cash = models.FloatField(initial=0)
    cash_after_dividend = models.FloatField(initial=0)
    daybreak_snapshot_error = models.LongStringField(blank=True)
    assigned_initial_cash = models.FloatField(initial=0)
    assigned_initial_shares = models.FloatField(initial=0)
    forecast_price_next_day = models.FloatField(blank=True)
    forecast_confidence_next_day = models.IntegerField(blank=True)
    forecast_survey_json = models.LongStringField(blank=True)
    realized_next_day_closing_price = models.FloatField(blank=True)
    forecast_bonus_earned = models.CurrencyField(initial=0)
    forecast_bonus_scored = models.BooleanField(initial=False)
    algo_belief_present = models.StringField(
        blank=True,
        label="Do you think an algorithmic trader was present in this market?",
        choices=[("yes", "Yes"), ("no", "No")],
        widget=widgets.RadioSelect,
    )
    algo_belief_confidence = models.IntegerField(
        blank=True,
        label="How confident are you in your answer?",
        choices=range(1, 6),
        widget=widgets.RadioSelect,
    )

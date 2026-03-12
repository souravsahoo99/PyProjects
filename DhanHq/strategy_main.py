# ============================================================
# STRATEGY MAIN
# Production Strategy Layer
# Compatible with SignalEngine (Checkpoint 4.0)
# ============================================================

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown


# ============================================================
# BASE STRATEGY CLASS
# ============================================================

class BaseStrategy:

    name = "BASE"

    def evaluate(self, context):
        return None


# ============================================================
# ORB STRATEGY
# ============================================================

class StrategyORB(BaseStrategy):

    name = "ORB"

    def evaluate(self, context):

        if not context["orb_ready"]:
            return None

        close = context["close"][0]

        orb_high = context["orb_high"]
        orb_low = context["orb_low"]

        if breakout(close, orb_high):
            return "BUY"

        if breakdown(close, orb_low):
            return "SELL"

        return None


# ============================================================
# VWAP DEVIATION STRATEGY
# ============================================================

class StrategyVWAPDeviation(BaseStrategy):

    name = "VWAP_DEV"

    def evaluate(self, context):

        df = context["df"]

        if df is None or len(df) < 20:
            return None

        try:
            bands = VWAPBands(df).calculate()
        except Exception:
            return None

        if bands is None:
            return None

        close = context["close"][0]

        upper = bands.get("upper1")
        lower = bands.get("lower1")

        if upper is None or lower is None:
            return None

        if close > upper:
            return "BUY"

        if close < lower:
            return "SELL"

        return None


# ============================================================
# MARKET PROFILE BREAK STRATEGY
# ============================================================

class StrategyMarketProfileBreak(BaseStrategy):

    name = "MP_BREAK"

    def evaluate(self, context):

        if not context["profile_ready"]:
            return None

        close = context["close"][0]

        vah = context["vah"]
        val = context["val"]

        if breakout(close, vah):
            return "BUY"

        if breakdown(close, val):
            return "SELL"

        return None


# ============================================================
# STRATEGY EXECUTION ENGINE
# ============================================================

class StrategyExecutor:

    def __init__(self):

        self.strategies = [

            StrategyORB(),
            StrategyVWAPDeviation(),
            StrategyMarketProfileBreak()

        ]

    def register(self, strategy):

        if strategy is None:
            return

        self.strategies.append(strategy)

    def run(self, context):

        if context is None:
            return None, None

        for strategy in self.strategies:

            try:
                result = strategy.evaluate(context)
            except Exception:
                continue

            if result:
                return strategy.name, result

        return None, None


# ============================================================
# ------------------------------------------------------------
# FUTURE STRATEGY TEMPLATES
# (PLACEHOLDER EXAMPLES)
# ------------------------------------------------------------
# ============================================================


# ============================================================
# MOMENTUM BURST STRATEGY
# ============================================================

class StrategyMomentumBurst(BaseStrategy):

    name = "MOMENTUM_BURST"

    def evaluate(self, context):

        close = context["close"]

        if close[0] is None or close[1] is None or close[2] is None:
            return None

        if close[0] > close[1] > close[2]:
            return "BUY"

        if close[0] < close[1] < close[2]:
            return "SELL"

        return None


# ============================================================
# TREND CONTINUATION STRATEGY
# ============================================================

class StrategyTrendContinuation(BaseStrategy):

    name = "TREND_CONT"

    def evaluate(self, context):

        close = context["close"]

        if close[0] is None or close[5] is None:
            return None

        if close[0] > close[5]:
            return "BUY"

        if close[0] < close[5]:
            return "SELL"

        return None


# ============================================================
# RANGE BREAK STRATEGY
# ============================================================

class StrategyRangeBreak(BaseStrategy):

    name = "RANGE_BREAK"

    def evaluate(self, context):

        high = context["high"]
        low = context["low"]
        close = context["close"]

        if high[1] is None or low[1] is None:
            return None

        range_high = high[1]
        range_low = low[1]

        if close[0] > range_high:
            return "BUY"

        if close[0] < range_low:
            return "SELL"

        return None


# ============================================================
# VOLUME SPIKE STRATEGY
# ============================================================

class StrategyVolumeSpike(BaseStrategy):

    name = "VOL_SPIKE"

    def evaluate(self, context):

        volume = context["volume"]

        if volume[0] is None or volume[1] is None:
            return None

        if volume[0] > volume[1] * 2:
            return "BUY"

        return None


# ============================================================
# REVERSAL PATTERN STRATEGY
# ============================================================

class StrategyReversalPattern(BaseStrategy):

    name = "REVERSAL"

    def evaluate(self, context):

        high = context["high"]
        low = context["low"]

        if high[0] is None or high[1] is None:
            return None

        if high[0] < high[1] and low[0] > low[1]:
            return "BUY"

        if high[0] > high[1] and low[0] < low[1]:
            return "SELL"

        return None


# ============================================================
# TIME WINDOW STRATEGY TEMPLATE
# ============================================================

class StrategyTimeWindow(BaseStrategy):

    name = "TIME_WINDOW"

    def evaluate(self, context):

        df = context["df"]

        if df is None or len(df) == 0:
            return None

        now = df.iloc[-1]["timestamp"]

        # placeholder example
        if now.hour == 10 and now.minute < 5:
            return None

        return None
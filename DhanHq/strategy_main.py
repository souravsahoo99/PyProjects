# ============================================================
# STRATEGY MAIN v1.1
# Production Strategy Layer
# Compatible with SignalEngine (Checkpoint 3.1)
# ============================================================

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown


# ============================================================
# BASE STRATEGY CLASS
# ============================================================

class BaseStrategy:
    """
    Base strategy interface.

    All strategies must implement evaluate(context)

    context dictionary contains:
        df
        open
        high
        low
        close
        volume

        orb_high
        orb_low
        orb_ready

        vah
        val
        profile_ready
    """

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
    """
    Central strategy runner.

    Responsible for:
        registering strategies
        executing them sequentially
        returning first valid signal
    """

    def __init__(self):

        self.strategies = [

            StrategyORB(),
            StrategyVWAPDeviation(),
            StrategyMarketProfileBreak()

        ]


    # --------------------------------------------------------
    # REGISTER NEW STRATEGY
    # --------------------------------------------------------

    def register(self, strategy):

        if strategy is None:
            return

        self.strategies.append(strategy)


    # --------------------------------------------------------
    # RUN STRATEGIES
    # --------------------------------------------------------

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
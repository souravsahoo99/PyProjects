# ============================================================
# STRATEGY MAIN v4.1
# Production Strategy Layer
# DataServant Compatible
# ============================================================

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown

from datetime import datetime, time
import asyncio


# ============================================================
# BASE STRATEGY CLASS
# ============================================================

class BaseStrategy:

    name = "BASE"

    REQUIRED_TIMEFRAMES = ["1m"]

    def evaluate(self, context):
        return None


    # --------------------------------------------------------
    # DATA SERVANT ACCESS
    # --------------------------------------------------------

    def get_servant(self, context):

        return context.get("servant")


    # --------------------------------------------------------
    # MULTI TF FETCH
    # --------------------------------------------------------

    def fetch_tf(self, context, exchange, token, timeframe):

        servant = self.get_servant(context)

        if servant is None:
            return None

        try:

            loop = asyncio.get_event_loop()

            if loop.is_running():

                future = asyncio.run_coroutine_threadsafe(
                    servant.get_candles(exchange, token, timeframe),
                    loop
                )

                return future.result(timeout=0.5)

            else:

                return loop.run_until_complete(
                    servant.get_candles(exchange, token, timeframe)
                )

        except Exception:

            return None


# ============================================================
# ORB STRATEGY
# ============================================================

class StrategyORB(BaseStrategy):

    name = "ORB"

    REQUIRED_TIMEFRAMES = ["1m"]

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

    REQUIRED_TIMEFRAMES = ["1m"]

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

    REQUIRED_TIMEFRAMES = ["1m"]

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
# DATA SERVANT STRATEGY 1
# Multi-Timeframe Trend
# ============================================================

class StrategyMTFTrend(BaseStrategy):

    name = "MTF_TREND"

    REQUIRED_TIMEFRAMES = ["1m"]

    def evaluate(self, context):

        token = context.get("token")
        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "5m")

        if buffer is None or len(buffer) < 5:
            return None

        trend = buffer.close[-1] > buffer.close[-5]

        close = context["close"][0]

        if trend and close > context["close"][1]:
            return "BUY"

        if not trend and close < context["close"][1]:
            return "SELL"

        return None


# ============================================================
# DATA SERVANT STRATEGY 2
# Higher TF Breakout
# ============================================================

class StrategyHTFBreakout(BaseStrategy):

    name = "HTF_BREAK"

    REQUIRED_TIMEFRAMES = ["1m"]

    def evaluate(self, context):

        token = context.get("token")
        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "15m")

        if buffer is None or len(buffer) < 2:
            return None

        high = buffer.high[-1]
        low = buffer.low[-1]

        price = context["close"][0]

        if price > high:
            return "BUY"

        if price < low:
            return "SELL"

        return None


# ============================================================
# DATA SERVANT STRATEGY 3
# VWAP Alignment
# ============================================================

class StrategyMTFVWAP(BaseStrategy):

    name = "MTF_VWAP"

    REQUIRED_TIMEFRAMES = ["1m"]

    def evaluate(self, context):

        token = context.get("token")
        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "3m")

        if buffer is None or len(buffer) < 20:
            return None

        import pandas as pd

        df = pd.DataFrame({
            "open": list(buffer.open),
            "high": list(buffer.high),
            "low": list(buffer.low),
            "close": list(buffer.close),
            "volume": list(buffer.volume)
        })

        bands = VWAPBands(df).calculate()

        if bands is None:
            return None

        price = context["close"][0]

        if price > bands["vwap"]:
            return "BUY"

        if price < bands["vwap"]:
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
            StrategyMarketProfileBreak(),

            # DataServant strategies
            StrategyMTFTrend(),
            StrategyHTFBreakout(),
            StrategyMTFVWAP()

        ]


    def register(self, strategy):

        if strategy is None:
            return

        self.strategies.append(strategy)


    def discover_required_timeframes(self):

        timeframes = set()

        for strategy in self.strategies:

            if hasattr(strategy, "REQUIRED_TIMEFRAMES"):

                for tf in strategy.REQUIRED_TIMEFRAMES:
                    timeframes.add(tf)

        if not timeframes:
            timeframes.add("1m")

        return sorted(list(timeframes))


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


#_
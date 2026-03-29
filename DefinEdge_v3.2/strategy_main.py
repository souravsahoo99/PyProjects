# ============================================================
# STRATEGY MAIN v6.1
# Production Strategy Layer
# Thread Safe Version
# ============================================================

import pandas as pd
from datetime import datetime
import pytz

from indicator_utils import (
    OpeningRangeIndicator,
    VWAPIndicator,
    MarketProfileIndicator
)

from strategy_utils import (
    VWAPBandState,
    VwapMemoryModule,
    TpoMemoryModule,
    breakout,
    breakdown,
    cross,
    cross_over,
    cross_under,
    higher_high,
    lower_low,
    trend_state,
    pivot_high,
    pivot_low,
    swing_high,
    swing_low,
    highest,
    lowest,
    bars_since,
    value_when
)


# ============================================================
# BASE STRATEGY
# ============================================================

class BaseStrategy:

    name = "BASE"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["PARENT", "CHILD", "BOTH"]
    INSTRUMENT_SCOPE = ["FUT", "OPT", "STOCK"]

    IST = pytz.timezone("Asia/Kolkata")


    def evaluate(self, context):
        return None


    # --------------------------------------------------------
    # SERVANT ACCESS
    # --------------------------------------------------------

    def get_servant(self, context):

        return context.get("servant")


    # --------------------------------------------------------
    # TOKEN ACCESS
    # --------------------------------------------------------

    def get_token(self, context):

        token = context.get("token")

        if token is None:
            token = context.get("symbol_token")

        return token


    # --------------------------------------------------------
    # MULTI TF FETCH
    # --------------------------------------------------------

    def fetch_tf(self, context, exchange, token, timeframe):

        servant = self.get_servant(context)

        if servant is None or token is None:
            return None

        try:

            return servant.get_cached(exchange, token, timeframe)

        except Exception:
            return None


    # --------------------------------------------------------
    # SESSION FILTER
    # --------------------------------------------------------

    def in_session(self, context, start, end):

        df = context.get("df")

        if df is None or len(df) == 0:
            return False

        try:

            ts = df.iloc[-1]["timestamp"]

            if not isinstance(ts, datetime):
                return False

            ts = ts.astimezone(self.IST)

            start_h, start_m = map(int, start.split(":"))
            end_h, end_m = map(int, end.split(":"))

            start_dt = ts.replace(hour=start_h, minute=start_m, second=0)
            end_dt = ts.replace(hour=end_h, minute=end_m, second=0)

            return start_dt <= ts <= end_dt

        except Exception:
            return False


# ============================================================
# ORB STRATEGY
# ============================================================

class StrategyORB(BaseStrategy):

    name = "ORB"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["PARENT"]
    INSTRUMENT_SCOPE = ["FUT", "STOCK"]


    def evaluate(self, context):

        if not self.in_session(context, "09:15", "09:30"):
            return None

        df = context.get("df")

        if df is None or len(df) < 3:
            return None

        try:

            indicator = OpeningRangeIndicator(
                timeframe="1m",
                instrument=context.get("symbol"),
                candle_buffer=df
            )

            orb = indicator.calculate()

        except Exception:
            return None

        if orb is None:
            return None

        close = context["close"][0]

        orb_high = orb.get("orbHigh")
        orb_low = orb.get("orbLow")

        if breakout(close, orb_high):
            return "BUY"

        if breakdown(close, orb_low):
            return "SELL"

        return None


# ============================================================
# VWAP DEVIATION
# ============================================================

class StrategyVWAPDeviation(BaseStrategy):

    name = "VWAP_DEV"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["PARENT"]
    INSTRUMENT_SCOPE = ["FUT", "STOCK"]


    def evaluate(self, context):

        if not self.in_session(context, "09:30", "11:30"):
            return None

        df = context.get("df")

        if df is None or len(df) < 20:
            return None

        try:

            indicator = VWAPBandState(
                timeframe="1m",
                instrument=context.get("symbol"),
                candle_buffer=df
            )

            bands = indicator.calculate()

        except Exception:
            return None

        if bands is None:
            return None

        close = context["close"][0]

        upper = bands.get("up1")
        lower = bands.get("dn1")

        if upper is None or lower is None:
            return None

        if close > upper:
            return "BUY"

        if close < lower:
            return "SELL"

        return None


# ============================================================
# MARKET PROFILE BREAK
# ============================================================

class StrategyMarketProfileBreak(BaseStrategy):

    name = "MP_BREAK"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["PARENT"]
    INSTRUMENT_SCOPE = ["FUT", "STOCK"]


    def evaluate(self, context):

        if not self.in_session(context, "09:30", "11:30"):
            return None

        df = context.get("df")

        if df is None or len(df) < 30:
            return None

        try:

            indicator = MarketProfileIndicator(
                timeframe="1m",
                instrument=context.get("symbol"),
                candle_buffer=df
            )

            profile = indicator.calculate()

        except Exception:
            return None

        if profile is None:
            return None

        close = context["close"][0]

        vah = profile.get("vah")
        val = profile.get("val")

        if breakout(close, vah):
            return "BUY"

        if breakdown(close, val):
            return "SELL"

        return None


# ============================================================
# MTF TREND
# ============================================================

class StrategyMTFTrend(BaseStrategy):

    name = "MTF_TREND"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["CHILD"]
    INSTRUMENT_SCOPE = ["OPT", "STOCK"]


    def evaluate(self, context):

        token = self.get_token(context)

        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "5m")

        if buffer is None or len(buffer.time) < 5:
            return None

        trend = buffer.close[-1] > buffer.close[-5]

        close = context["close"][0]

        if trend and close > context["close"][1]:
            return "BUY"

        if not trend and close < context["close"][1]:
            return "SELL"

        return None


# ============================================================
# HIGHER TF BREAKOUT
# ============================================================

class StrategyHTFBreakout(BaseStrategy):

    name = "HTF_BREAK"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["CHILD"]
    INSTRUMENT_SCOPE = ["OPT", "STOCK"]


    def evaluate(self, context):

        token = self.get_token(context)

        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "15m")

        if buffer is None or len(buffer.time) < 2:
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
# MTF VWAP ALIGNMENT
# ============================================================

class StrategyMTFVWAP(BaseStrategy):

    name = "MTF_VWAP"

    REQUIRED_TIMEFRAMES = ["1m"]

    NODE_SCOPE = ["CHILD"]
    INSTRUMENT_SCOPE = ["OPT", "STOCK"]


    def evaluate(self, context):

        token = self.get_token(context)

        exchange = "NSE"

        buffer = self.fetch_tf(context, exchange, token, "3m")

        if buffer is None or len(buffer.time) < 20:
            return None

        df = pd.DataFrame({

            "open": list(buffer.open),
            "high": list(buffer.high),
            "low": list(buffer.low),
            "close": list(buffer.close),
            "volume": list(buffer.volume)

        })

        try:

            indicator = VWAPIndicator(
                timeframe="3m",
                instrument=context.get("symbol"),
                candle_buffer=df
            )

            vwap_data = indicator.calculate()

        except Exception:
            return None

        if vwap_data is None:
            return None

        price = context["close"][0]

        vwap = vwap_data.get("vwap")

        if price > vwap:
            return "BUY"

        if price < vwap:
            return "SELL"

        return None


# ============================================================
# STRATEGY EXECUTOR
# ============================================================

class StrategyExecutor:

    def __init__(self):

        self.strategies = [

            StrategyORB(),
            StrategyVWAPDeviation(),
            StrategyMarketProfileBreak(),

            StrategyMTFTrend(),
            StrategyHTFBreakout(),
            StrategyMTFVWAP()

        ]


    def register(self, strategy):

        if strategy is None:
            return

        if not hasattr(strategy, "evaluate"):
            return

        self.strategies.append(strategy)


    def discover_required_timeframes(self):

        timeframes = set()

        for strategy in self.strategies:

            tfs = getattr(strategy, "REQUIRED_TIMEFRAMES", None)

            if tfs:
                timeframes.update(tfs)

        if not timeframes:
            timeframes.add("1m")

        return sorted(timeframes)


    def get_strategies(self, instrument_type=None, node_scope=None):

        filtered = []

        for strategy in self.strategies:

            inst_scope = getattr(strategy, "INSTRUMENT_SCOPE", None)
            node_scope_s = getattr(strategy, "NODE_SCOPE", None)

            inst_ok = (
                inst_scope is None
                or instrument_type in inst_scope
            )

            node_ok = (
                node_scope_s is None
                or node_scope in node_scope_s
                or "BOTH" in node_scope_s
            )

            if inst_ok and node_ok:
                filtered.append(strategy)

        return filtered


    def run(self, context, strategies=None):

        if context is None:
            return None, None

        if strategies is None:
            strategies = self.strategies

        for strategy in strategies:

            try:

                result = strategy.evaluate(context)

                if result not in ["BUY", "SELL", None]:
                    continue

            except Exception:
                continue

            if result:
                return strategy.name, result

        return None, None
    


#_
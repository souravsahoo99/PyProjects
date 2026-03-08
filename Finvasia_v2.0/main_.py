# ============================================================
# SIGNAL ENGINE
# ============================================================

import time
import asyncio
import pandas as pd
from datetime import datetime

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown


# ============================================================
# GLOBAL SIGNAL BUS
# ============================================================

SIGNAL = {

    "state": False,

    "exchange": None,
    "symbol": None,
    "token": None,

    "side": None,
    "entry_price": None,
    "signal_time": None,
    "strategy": None
}


# ============================================================
# SIGNAL ENGINE
# ============================================================

class SignalEngine:
    """
    SignalEngine evaluates strategies for ONE instrument pipeline
    and publishes signals to the global SIGNAL bus.
    """

    def __init__(self, market_data, exchange, symbol, token):

        self.market_data = market_data

        self.exchange = exchange
        self.symbol = symbol
        self.token = token

        # track last processed candle
        self.last_candle_time = None

        # ORB state
        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False

        # Market profile levels
        self.vah = None
        self.val = None
        self.profile_ready = False


    # --------------------------------------------------------
    # BUILD DATAFRAME FROM BUFFER
    # --------------------------------------------------------

    def _build_dataframe(self, buffer):

        if buffer is None:
            return None

        if len(buffer) == 0:
            return None

        df = pd.DataFrame({

            "timestamp": list(buffer.time),
            "open": list(buffer.open),
            "high": list(buffer.high),
            "low": list(buffer.low),
            "close": list(buffer.close),
            "volume": list(buffer.volume)

        })

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        return df


    # --------------------------------------------------------
    # ORB CALCULATION
    # --------------------------------------------------------

    def _update_orb(self, df):

        now = df.iloc[-1]["timestamp"]

        start = now.replace(hour=9, minute=15, second=0)
        end = now.replace(hour=9, minute=30, second=0)

        if now <= end:

            session_df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

            if len(session_df) > 0:

                self.orb_high = session_df["high"].max()
                self.orb_low = session_df["low"].min()

        if now > end and self.orb_high is not None:
            self.orb_ready = True


    # --------------------------------------------------------
    # LOAD MARKET PROFILE
    # --------------------------------------------------------

    def _load_market_profile(self, df):

        if self.profile_ready:
            return

        profile = MarketProfile(df)

        res = profile.calculate()

        if res is None:
            return

        value_area = res["value_area"]

        if len(value_area) == 0:
            return

        self.vah = max(value_area)
        self.val = min(value_area)

        self.profile_ready = True


    # --------------------------------------------------------
    # STRATEGY 1 : ORB
    # --------------------------------------------------------

    def _strategy_orb(self, close):

        if not self.orb_ready:
            return None

        if breakout(close, self.orb_high):
            return "BUY"

        if breakdown(close, self.orb_low):
            return "SELL"

        return None


    # --------------------------------------------------------
    # STRATEGY 2 : VWAP DEVIATION
    # --------------------------------------------------------

    def _strategy_vwap(self, df):

        vwap = VWAPBands(df)

        bands = vwap.calculate()

        if bands is None:
            return None

        close = df.iloc[-1]["close"]

        if close > bands["upper1"]:
            return "BUY"

        if close < bands["lower1"]:
            return "SELL"

        return None


    # --------------------------------------------------------
    # STRATEGY 3 : MARKET PROFILE BREAKOUT
    # --------------------------------------------------------

    def _strategy_market_profile(self, close):

        if not self.profile_ready:
            return None

        if breakout(close, self.vah):
            return "BUY"

        if breakdown(close, self.val):
            return "SELL"

        return None


    # --------------------------------------------------------
    # PUBLISH SIGNAL
    # --------------------------------------------------------

    def _publish_signal(self, side, price, timestamp, strategy):

        global SIGNAL

        if SIGNAL["state"]:
            return

        SIGNAL["state"] = True

        SIGNAL["exchange"] = self.exchange
        SIGNAL["symbol"] = self.symbol
        SIGNAL["token"] = self.token

        SIGNAL["side"] = side
        SIGNAL["entry_price"] = price
        SIGNAL["signal_time"] = timestamp
        SIGNAL["strategy"] = strategy

        print(f"[SIGNAL] {self.symbol} → {side} | {strategy} | {price}")


    # --------------------------------------------------------
    # STRATEGY EVALUATION
    # --------------------------------------------------------

    def _evaluate(self, df):

        close = df.iloc[-1]["close"]

        timestamp = int(df.iloc[-1]["timestamp"].timestamp())

        res = self._strategy_orb(close)

        if res:
            self._publish_signal(res, close, timestamp, "ORB")
            return

        res = self._strategy_vwap(df)

        if res:
            self._publish_signal(res, close, timestamp, "VWAP_DEV")
            return

        res = self._strategy_market_profile(close)

        if res:
            self._publish_signal(res, close, timestamp, "MP_BREAK")
            return


    # --------------------------------------------------------
    # MAIN SIGNAL LOOP
    # --------------------------------------------------------

    async def run(self):

        while True:

            buffer = self.market_data.get("1m")

            if buffer is None or len(buffer) == 0:

                await asyncio.sleep(0.2)
                continue

            candle_time = buffer.time[-1]

            if candle_time == self.last_candle_time:

                await asyncio.sleep(0.2)
                continue

            self.last_candle_time = candle_time

            df = self._build_dataframe(buffer)

            if df is None:

                await asyncio.sleep(0.2)
                continue

            self._update_orb(df)

            self._load_market_profile(df)

            self._evaluate(df)

            await asyncio.sleep(0.2)


#_#_
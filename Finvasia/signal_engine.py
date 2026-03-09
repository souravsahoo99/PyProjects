# ============================================================
# SIGNAL ENGINE
# Production Grade
# ============================================================

import asyncio
import pandas as pd
import threading

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown


# ============================================================
# GLOBAL SIGNAL STORAGE
# ============================================================

SIGNALS = []
SIGNAL_LOCK = threading.Lock()


# ============================================================
# SIGNAL PUBLISHER
# Controls which signals are allowed onto the global bus
# ============================================================

class SignalPublisher:

    def __init__(
        self,
        parent_token=None,
        child_token=None,
        ce_token=None,
        pe_token=None,
        product_type=None,
        allowed_strategies=None
    ):

        self.parent_token = parent_token
        self.child_token = child_token
        self.ce_token = ce_token
        self.pe_token = pe_token
        self.product_type = product_type
        self.allowed_strategies = allowed_strategies


    # --------------------------------------------------------
    # TOKEN VALIDATION
    # --------------------------------------------------------

    def _is_allowed_token(self, token):

        if self.product_type == "OPT":

            if token == self.parent_token:
                return True

            elif token == self.ce_token:
                return True

            elif token == self.pe_token:
                return True

            else:
                return False

        elif self.product_type == "FUT":

            if token == self.parent_token:
                return True

            elif token == self.child_token:
                return True

            else:
                return False

        elif self.product_type == "SPOT":

            if token == self.parent_token:
                return True

            else:
                return False

        else:

            return True


    # --------------------------------------------------------
    # STRATEGY VALIDATION
    # --------------------------------------------------------

    def _is_allowed_strategy(self, strategy):

        if self.allowed_strategies is None:
            return True

        elif strategy in self.allowed_strategies:
            return True

        else:
            return False


    # --------------------------------------------------------
    # PUBLICATION DECISION
    # --------------------------------------------------------

    def allow_publish(self, token, strategy):

        token_ok = self._is_allowed_token(token)
        strategy_ok = self._is_allowed_strategy(strategy)

        if token_ok is True and strategy_ok is True:
            return True

        else:
            return False


# ============================================================
# SIGNAL ENGINE
# ============================================================

class SignalEngine:

    def __init__(self, market_data, symbol, token, publisher=None):

        self.market_data = market_data
        self.symbol = symbol
        self.token = token

        # Signal publisher injected from main.py
        self.publisher = publisher

        self.last_candle_time = None
        self.last_signal_key = None

        # ORB
        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False

        # Market Profile
        self.vah = None
        self.val = None
        self.profile_ready = False


    # --------------------------------------------------------
    # BUILD DATAFRAME
    # --------------------------------------------------------

    def _build_dataframe(self, buffer):

        if buffer is None or len(buffer) == 0:
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

        df = df.dropna()

        if len(df) == 0:
            return None

        return df


    # --------------------------------------------------------
    # ORB UPDATE
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
    # STRATEGY 1 — ORB
    # --------------------------------------------------------

    def _strategy_orb(self, close):

        if not self.orb_ready:
            return None

        if breakout(close, self.orb_high):
            return "BUY"

        elif breakdown(close, self.orb_low):
            return "SELL"

        else:
            return None


    # --------------------------------------------------------
    # STRATEGY 2 — VWAP DEVIATION
    # --------------------------------------------------------

    def _strategy_vwap(self, df):

        vwap = VWAPBands(df)
        bands = vwap.calculate()

        if bands is None:
            return None

        close = df.iloc[-1]["close"]

        if close > bands["upper1"]:
            return "BUY"

        elif close < bands["lower1"]:
            return "SELL"

        else:
            return None


    # --------------------------------------------------------
    # STRATEGY 3 — MARKET PROFILE
    # --------------------------------------------------------

    def _strategy_market_profile(self, close):

        if not self.profile_ready:
            return None

        if breakout(close, self.vah):
            return "BUY"

        elif breakdown(close, self.val):
            return "SELL"

        else:
            return None


    # --------------------------------------------------------
    # PUBLISH SIGNAL
    # --------------------------------------------------------

    def _publish_signal(self, side, price, timestamp, strategy):

        global SIGNALS

        signal_key = f"{self.symbol}_{strategy}_{timestamp}"

        if signal_key == self.last_signal_key:
            return

        # ----------------------------------------------------
        # SIGNAL PUBLISHER FILTER
        # ----------------------------------------------------

        if self.publisher is not None:

            allowed = self.publisher.allow_publish(self.token, strategy)

            if allowed is False:
                return

        # ----------------------------------------------------
        # BUILD SIGNAL
        # ----------------------------------------------------

        signal_dict = {

            "symbol": self.symbol,
            "token": self.token,

            "side": side,
            "entry_price": price,
            "signal_time": timestamp,
            "strategy": strategy
        }

        with SIGNAL_LOCK:

            SIGNALS.append(signal_dict)

            if len(SIGNALS) > 300:
                SIGNALS.pop(0)

        self.last_signal_key = signal_key

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
    # MAIN LOOP
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





            
#_#_#_#_#_
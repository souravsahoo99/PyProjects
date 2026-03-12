# ============================================================
# SIGNAL ENGINE
# Production Grade (Checkpoint 2.3 Compatible)
# ============================================================

import asyncio
import pandas as pd
import threading

from indicator_utils import VWAPBands, MarketProfile
from strategy_utils import breakout, breakdown


# ============================================================
# GLOBAL SIGNAL BUS
# ============================================================

SIGNALS = []
SIGNAL_LOCK = threading.Lock()


# ============================================================
# SIGNAL PUBLISHER
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


    def _is_allowed_token(self, token):

        if self.product_type == "OPT":
            return token in [self.parent_token, self.ce_token, self.pe_token]

        if self.product_type == "FUT":
            return token in [self.parent_token, self.child_token]

        if self.product_type in ["SPOT", "STOCK"]:
            return token == self.parent_token

        return True


    def _is_allowed_strategy(self, strategy):

        if self.allowed_strategies is None:
            return True

        return strategy in self.allowed_strategies


    def allow_publish(self, token, strategy):

        return (
            self._is_allowed_token(token)
            and self._is_allowed_strategy(strategy)
        )


# ============================================================
# SIGNAL ENGINE
# ============================================================

class SignalEngine:

    def __init__(self, market_data, symbol, token, publisher=None):

        self.market_data = market_data
        self.symbol = symbol
        self.token = token

        self.publisher = publisher

        # local signal history (for chart)
        self.signal_history = []
        self._signal_history_limit = 2000
        self._history_lock = threading.Lock()

        # state tracking
        self.last_candle_time = None
        self.last_signal_key = None

        # ORB state
        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False

        # market profile state
        self.vah = None
        self.val = None
        self.profile_ready = False

        self._running = True


    # ========================================================
    # ACCESSORS
    # ========================================================

    def get_signal_history(self):

        with self._history_lock:
            return list(self.signal_history)


    def get_orb_levels(self):

        if not self.orb_ready:
            return None, None

        return self.orb_high, self.orb_low


    def get_profile_levels(self):

        if not self.profile_ready:
            return None, None

        return self.vah, self.val


    # ========================================================
    # DATAFRAME BUILDER
    # ========================================================

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

        return df.dropna()


    # ========================================================
    # ORB UPDATE
    # ========================================================

    def _update_orb(self, df):

        now = df.iloc[-1]["timestamp"]

        start = now.replace(hour=9, minute=15, second=0)
        end = now.replace(hour=9, minute=30, second=0)

        if now <= end:

            session_df = df[
                (df["timestamp"] >= start)
                & (df["timestamp"] <= end)
            ]

            if len(session_df) > 0:

                self.orb_high = session_df["high"].max()
                self.orb_low = session_df["low"].min()

        if now > end and self.orb_high is not None:

            self.orb_ready = True


    # ========================================================
    # MARKET PROFILE
    # ========================================================

    def _load_market_profile(self, df):

        if self.profile_ready:
            return

        if len(df) < 30:
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


    # ========================================================
    # STRATEGY EVALUATION
    # ========================================================

    def _evaluate(self, df):

        close = df.iloc[-1]["close"]
        timestamp = int(df.iloc[-1]["timestamp"].timestamp())

        # ORB strategy
        if self.orb_ready:

            if breakout(close, self.orb_high):

                self._publish_signal(
                    "BUY",
                    close,
                    timestamp,
                    "ORB"
                )

                return

            if breakdown(close, self.orb_low):

                self._publish_signal(
                    "SELL",
                    close,
                    timestamp,
                    "ORB"
                )

                return

        # VWAP deviation
        bands = VWAPBands(df).calculate()

        if bands:

            if close > bands["upper1"]:

                self._publish_signal(
                    "BUY",
                    close,
                    timestamp,
                    "VWAP_DEV"
                )

                return

            if close < bands["lower1"]:

                self._publish_signal(
                    "SELL",
                    close,
                    timestamp,
                    "VWAP_DEV"
                )

                return

        # Market profile break
        if self.profile_ready:

            if breakout(close, self.vah):

                self._publish_signal(
                    "BUY",
                    close,
                    timestamp,
                    "MP_BREAK"
                )

                return

            if breakdown(close, self.val):

                self._publish_signal(
                    "SELL",
                    close,
                    timestamp,
                    "MP_BREAK"
                )


    # ========================================================
    # SIGNAL PUBLICATION
    # ========================================================

    def _publish_signal(self, side, price, timestamp, strategy):

        global SIGNALS

        signal_key = f"{self.symbol}_{strategy}_{timestamp}"

        if signal_key == self.last_signal_key:
            return

        if self.publisher:

            allowed = self.publisher.allow_publish(
                self.token,
                strategy
            )

            if not allowed:
                return

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

        with self._history_lock:

            self.signal_history.append(signal_dict)

            if len(self.signal_history) > self._signal_history_limit:
                self.signal_history.pop(0)

        self.last_signal_key = signal_key

        print(f"[SIGNAL] {self.symbol} → {side} | {strategy} | {price}")


    # ========================================================
    # MAIN LOOP
    # ========================================================

    async def run(self):

        while self._running:

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


    # ========================================================
    # STOP ENGINE
    # ========================================================

    def stop(self):

        self._running = False




            
#_#_#_#_#_#_#_#_#_#_
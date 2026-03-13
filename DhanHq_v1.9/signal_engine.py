# ============================================================
# SIGNAL ENGINE
# Production Grade (Checkpoint 5.x Compatible)
# Strategy Driven Pipelines + DataServant Layer
# ============================================================

import asyncio
import pandas as pd
import threading

from strategy_main import StrategyExecutor
from pineseries_adapter import SeriesAdapter
from indicator_utils import MarketProfile
from market_data import MarketDataManager


# ============================================================
# GLOBAL SIGNAL BUS
# ============================================================

SIGNALS = []
SIGNAL_LOCK = threading.Lock()


# ============================================================
# DATA SERVANT
# Unified Candle Data Gateway
# ============================================================

class DataServant:

    def __init__(self, engine):

        self.engine = engine

        # (exchange, token) → MarketDataManager
        self.pipeline_registry = {}

        self._lock = asyncio.Lock()


    async def get_candles(self, exchange, token, timeframe):

        key = (exchange, token)

        async with self._lock:

            md = self.pipeline_registry.get(key)

            if md is None:

                md = MarketDataManager(
                    self.engine,
                    exchange,
                    token,
                    required_timeframes=[timeframe]
                )

                self.pipeline_registry[key] = md

                asyncio.create_task(md.start())

                return md.get(timeframe)

            if timeframe not in md.rest_agg.buffers:

                existing = list(md.rest_agg.buffers.keys())
                new_tfs = list(set(existing + [timeframe]))

                await md.stop()

                md = MarketDataManager(
                    self.engine,
                    exchange,
                    token,
                    required_timeframes=new_tfs
                )

                self.pipeline_registry[key] = md

                asyncio.create_task(md.start())

        return md.get(timeframe)


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

    def __init__(self, engine, market_data, symbol, token, publisher=None):

        self.engine = engine
        self.market_data = market_data

        self.symbol = symbol
        self.token = token

        self.publisher = publisher

        # ----------------------------------------------------
        # DATA SERVANT
        # ----------------------------------------------------

        # injected by InstrumentNode
        self.servant = None

        # ----------------------------------------------------
        # STRATEGY EXECUTOR
        # ----------------------------------------------------

        self.strategy_engine = StrategyExecutor()

        # ----------------------------------------------------
        # DISCOVER REQUIRED PIPELINES
        # ----------------------------------------------------

        self.required_timeframes = self._discover_required_timeframes()

        # ----------------------------------------------------
        # LOCAL SIGNAL HISTORY
        # ----------------------------------------------------

        self.signal_history = []
        self._signal_history_limit = 2000
        self._history_lock = threading.Lock()

        # ----------------------------------------------------
        # STATE TRACKING
        # ----------------------------------------------------

        self.last_candle_time = None
        self.last_signal_key = None

        # ----------------------------------------------------
        # ORB STATE
        # ----------------------------------------------------

        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False

        # ----------------------------------------------------
        # MARKET PROFILE STATE
        # ----------------------------------------------------

        self.vah = None
        self.val = None
        self.profile_ready = False

        self._running = True


    def _discover_required_timeframes(self):

        required = set()

        strategies = getattr(self.strategy_engine, "strategies", [])

        for strat in strategies:

            tfs = getattr(strat, "REQUIRED_TIMEFRAMES", None)

            if tfs:
                required.update(tfs)

        if not required:
            required.add("1m")

        return sorted(required)


    def get_required_timeframes(self):

        return list(self.required_timeframes)


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


    def _evaluate(self, df, buffer):

        timestamp = int(df.iloc[-1]["timestamp"].timestamp())

        adapter = SeriesAdapter({

            "open": buffer.open,
            "high": buffer.high,
            "low": buffer.low,
            "close": buffer.close,
            "volume": buffer.volume

        })

        context = {

            "df": df,

            "open": adapter.open(),
            "high": adapter.high(),
            "low": adapter.low(),
            "close": adapter.close(),
            "volume": adapter.volume(),

            "orb_high": self.orb_high,
            "orb_low": self.orb_low,
            "orb_ready": self.orb_ready,

            "vah": self.vah,
            "val": self.val,
            "profile_ready": self.profile_ready,

            "servant": self.servant
        }

        strategy, side = self.strategy_engine.run(context)

        if side:

            price = context["close"][0]

            self._publish_signal(
                side,
                price,
                timestamp,
                strategy
            )


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

            self._evaluate(df, buffer)

            await asyncio.sleep(0.2)


    def stop(self):

        self._running = False


            
#_#_#_#_#_#_#_#_#_#_
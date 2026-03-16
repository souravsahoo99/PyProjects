# ============================================================
# SIGNAL ENGINE v3.8
# Strategy Driven Pipelines + Shared DataServant Layer
# Node-Scope + Instrument-Scope Compatible
# Strategy Timeout Guard Enabled (Checkpoint 6.1)
# ============================================================

import asyncio
import pandas as pd
import threading
import concurrent.futures

from strategy_main import StrategyExecutor
from pineseries_adapter import SeriesAdapter
from indicator_utils import MarketProfile
from market_data import MarketDataManager
from data_servant import DataServant


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

    def __init__(
        self,
        engine,
        market_data,
        symbol,
        token,
        publisher=None,
        instrument_type=None,
        node_scope="PARENT"
    ):

        self.engine = engine
        self.market_data = market_data

        self.symbol = symbol
        self.token = token

        self.publisher = publisher

        self.instrument_type = instrument_type
        self.node_scope = node_scope

        self.servant = None

        self.strategy_engine = StrategyExecutor()

        self.strategies = self.strategy_engine.get_strategies(
            instrument_type=self.instrument_type,
            node_scope=self.node_scope
        )

        self.required_timeframes = self._discover_required_timeframes()

        self.signal_history = []
        self._signal_history_limit = 2000
        self._history_lock = threading.Lock()

        self.last_candle_time = None
        self.last_signal_key = None

        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False

        self.vah = None
        self.val = None
        self.profile_ready = False

        self._running = True

        # ----------------------------------------------------
        # STRATEGY EXECUTION THREAD POOL
        # ----------------------------------------------------

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)


    # ========================================================
    # DISCOVER REQUIRED TIMEFRAMES
    # ========================================================

    def _discover_required_timeframes(self):

        required = set()

        for strat in self.strategies:

            tfs = getattr(strat, "REQUIRED_TIMEFRAMES", None)

            if tfs:
                required.update(tfs)

        if not required:
            required.add("1m")

        return sorted(required)


    def get_required_timeframes(self):

        return list(self.required_timeframes)


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
    # SAFE STRATEGY EXECUTION
    # ========================================================

    def _safe_run_strategy(self, context):

        try:

            future = self._executor.submit(
                self.strategy_engine.run,
                context,
                self.strategies
            )

            return future.result(timeout=0.05)

        except concurrent.futures.TimeoutError:

            print(f"[STRATEGY TIMEOUT] {self.symbol}")

            return None, None

        except Exception:

            return None, None


    # ========================================================
    # STRATEGY EXECUTION
    # ========================================================

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

            "servant": self.servant,
            "token": self.token
        }

        strategy, side = self._safe_run_strategy(context)

        if side:

            price = context["close"][0]

            self._publish_signal(
                side,
                price,
                timestamp,
                strategy
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

        idle_sleep = 0.8
        active_sleep = 0.05

        while self._running:

            buffer = self.market_data.get("1m")

            if buffer is None or len(buffer) == 0:

                await asyncio.sleep(idle_sleep)
                continue

            candle_time = buffer.time[-1]

            if candle_time == self.last_candle_time:

                await asyncio.sleep(idle_sleep)
                continue

            self.last_candle_time = candle_time

            df = self._build_dataframe(buffer)

            if df is None:

                await asyncio.sleep(active_sleep)
                continue

            self._update_orb(df)
            self._load_market_profile(df)

            self._evaluate(df, buffer)

            await asyncio.sleep(active_sleep)


    # ========================================================
    # STOP ENGINE
    # ========================================================

    def stop(self):

        self._running = False




            
#_#_#_#_#_#_#_#_#_#_#
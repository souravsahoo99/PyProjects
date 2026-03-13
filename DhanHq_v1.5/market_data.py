# ============================================================
# MARKET DATA MANAGER
# Production Grade (Step-1 Strategy Driven Pipelines)
# Checkpoint 4.x Compatible
# ============================================================

import time
import asyncio
import threading
import pandas as pd
from collections import deque


# ============================================================
# CANDLE BUFFER
# ============================================================

class CandleBuffer:

    def __init__(self, maxlen=500):

        self.open = deque(maxlen=maxlen)
        self.high = deque(maxlen=maxlen)
        self.low = deque(maxlen=maxlen)
        self.close = deque(maxlen=maxlen)
        self.volume = deque(maxlen=maxlen)
        self.time = deque(maxlen=maxlen)

    def append(self, o, h, l, c, v, t):

        self.open.append(o)
        self.high.append(h)
        self.low.append(l)
        self.close.append(c)
        self.volume.append(v)
        self.time.append(t)

    def __len__(self):

        return len(self.close)

    def last(self):

        if len(self.close) == 0:
            return None

        return {
            "open": self.open[-1],
            "high": self.high[-1],
            "low": self.low[-1],
            "close": self.close[-1],
            "volume": self.volume[-1],
            "time": self.time[-1],
        }


# ============================================================
# TICK CANDLE AGGREGATOR
# ============================================================

class TickCandleAggregator:

    def __init__(self, maxlen=500):

        self.timeframes = {
            "10s": 10,
            "15s": 15,
            "30s": 30,
        }

        self.buffers = {}
        self.current = {}

        for tf in self.timeframes:

            self.buffers[tf] = CandleBuffer(maxlen)

            self.current[tf] = {
                "start": None,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
            }

        self._lock = threading.Lock()

    def update_tick(self, price, timestamp=None):

        if timestamp is None:
            timestamp = int(time.time())

        with self._lock:

            for tf, seconds in self.timeframes.items():

                self._update_tf(tf, seconds, price, timestamp)

    def _update_tf(self, tf, seconds, price, ts):

        bucket = ts - (ts % seconds)

        candle = self.current[tf]

        if candle["start"] is None:

            candle["start"] = bucket
            candle["open"] = price
            candle["high"] = price
            candle["low"] = price
            candle["close"] = price
            return

        if bucket == candle["start"]:

            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            return

        if bucket > candle["start"]:

            self.buffers[tf].append(
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                0,
                candle["start"],
            )

            candle["start"] = bucket
            candle["open"] = price
            candle["high"] = price
            candle["low"] = price
            candle["close"] = price

    def get(self, tf):

        return self.buffers.get(tf)


# ============================================================
# REST CANDLE AGGREGATOR
# ============================================================

class RestCandleAggregator:

    def __init__(self, engine, exchange, token, required_timeframes=None, maxlen=500):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        if required_timeframes is None:
            required_timeframes = ["1m"]

        self.required_timeframes = set(required_timeframes)

        # -----------------------------------------------------
        # DYNAMIC BUFFER CREATION
        # -----------------------------------------------------

        self.buffers = {}

        for tf in self.required_timeframes:
            self.buffers[tf] = CandleBuffer(maxlen)

        self._last_timestamp = {k: None for k in self.buffers}

        self._running = False
        self._tasks = []

    # ---------------------------------------------------------
    # STORE CANDLE
    # ---------------------------------------------------------

    def _store_candle(self, tf, candle):

        ts = int(pd.to_datetime(candle["timestamp"]).timestamp())

        last_ts = self._last_timestamp[tf]

        if last_ts is not None and ts <= last_ts:
            return

        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])
        c = float(candle["close"])
        v = float(candle["volume"])

        self.buffers[tf].append(o, h, l, c, v, ts)

        self._last_timestamp[tf] = ts

    # ---------------------------------------------------------
    # PROCESS RESPONSE
    # ---------------------------------------------------------

    def _process_response(self, tf, df):

        if df is None or len(df) == 0:
            return

        for _, row in df.iterrows():

            self._store_candle(tf, row)

    # ---------------------------------------------------------
    # INTRADAY PIPELINE
    # ---------------------------------------------------------

    async def _pipeline(self, tf, interval):

        interval_seconds = interval * 60

        while self._running:

            try:

                df = self.engine.get_ohlc(
                    exchange=self.exchange,
                    token=self.token,
                    interval=interval,
                )

                self._process_response(tf, df)

            except Exception as e:

                print(f"[REST PIPELINE ERROR] {tf}: {e}")

            now = time.time()

            sleep_time = interval_seconds - (now % interval_seconds)

            await asyncio.sleep(sleep_time)

    # ---------------------------------------------------------
    # DAILY PIPELINE
    # ---------------------------------------------------------

    async def _daily_pipeline(self):

        while self._running:

            try:

                df = self.engine.get_ohlc(
                    exchange=self.exchange,
                    token=self.token,
                    interval="1D",
                )

                if df is not None and len(df) > 0:

                    for _, candle in df.iterrows():

                        ts = int(pd.to_datetime(candle["timestamp"]).timestamp())

                        o = float(candle["open"])
                        h = float(candle["high"])
                        l = float(candle["low"])
                        c = float(candle["close"])
                        v = float(candle["volume"])

                        self.buffers["1D"].append(o, h, l, c, v, ts)

            except Exception as e:

                print("[DAILY PIPELINE ERROR]", e)

            await asyncio.sleep(3600)

    # ---------------------------------------------------------
    # START
    # ---------------------------------------------------------

    async def start(self):

        if self._running:
            return

        self._running = True

        loop = asyncio.get_running_loop()

        # intraday pipelines
        if "1m" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._pipeline("1m", 1)))

        if "3m" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._pipeline("3m", 3)))

        if "5m" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._pipeline("5m", 5)))

        if "30m" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._pipeline("30m", 30)))

        if "1D" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._daily_pipeline()))

    # ---------------------------------------------------------
    # STOP
    # ---------------------------------------------------------

    async def stop(self):

        if not self._running:
            return

        self._running = False

        for t in self._tasks:
            t.cancel()

        for t in self._tasks:

            try:
                await t
            except asyncio.CancelledError:
                pass

    def get(self, tf):

        return self.buffers.get(tf)


# ============================================================
# MARKET DATA MANAGER
# ============================================================

class MarketDataManager:

    def __init__(self, engine, exchange, token, required_timeframes=None):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        if required_timeframes is None:
            required_timeframes = ["1m"]

        # register router
        self.engine.market_data_map[f"{exchange}|{token}"] = self

        self.tick_queue = asyncio.Queue(maxsize=2000)

        self.tick_agg = TickCandleAggregator()

        self.rest_agg = RestCandleAggregator(
            engine,
            exchange,
            token,
            required_timeframes
        )

        self._tick_task = None
        self._running = False

    async def _tick_worker(self):

        while self._running:

            try:

                price = await asyncio.wait_for(
                    self.tick_queue.get(),
                    timeout=1,
                )

                if price is None:
                    continue

                self.tick_agg.update_tick(price)

            except asyncio.TimeoutError:
                continue

    async def start(self):

        if self._running:
            return

        self._running = True

        await self.rest_agg.start()

        loop = asyncio.get_running_loop()

        self._tick_task = loop.create_task(self._tick_worker())

    async def stop(self):

        self._running = False

        await self.rest_agg.stop()

        if self._tick_task:
            self._tick_task.cancel()

        key = f"{self.exchange}|{self.token}"

        if key in self.engine.market_data_map:
            del self.engine.market_data_map[key]

    def get(self, timeframe):

        if timeframe in ["10s", "15s", "30s"]:
            return self.tick_agg.get(timeframe)

        return self.rest_agg.get(timeframe)




#_#_#_#_#_#
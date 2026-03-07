import time
import asyncio
import threading
from collections import deque



# ============================================================
# CANDLE BUFFER (PINESCRIPT STYLE STORAGE)
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
            "time": self.time[-1]
        }



# ============================================================
# TICK → MICRO CANDLE AGGREGATOR
# (10s / 15s / 30s)
# ============================================================

class TickCandleAggregator:

    def __init__(self, maxlen=500):

        self.timeframes = {
            "10s": 10,
            "15s": 15,
            "30s": 30
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
                "close": None
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


        elif bucket == candle["start"]:

            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price


        elif bucket > candle["start"]:

            self.buffers[tf].append(
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                0,
                candle["start"]
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
# (1m / 3m / 5m)
# Independent pipelines
# Timestamp validation + backfill
# ============================================================

class RestCandleAggregator:

    def __init__(self, engine, exchange, token, maxlen=500):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        self.buffers = {
            "1m": CandleBuffer(maxlen),
            "3m": CandleBuffer(maxlen),
            "5m": CandleBuffer(maxlen)
        }

        self._last_timestamp = {
            "1m": None,
            "3m": None,
            "5m": None
        }

        self._running = False
        self._tasks = []


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


    def _process_response(self, tf, df):

        if df is None or len(df) == 0:

            return

        for _, row in df.iterrows():

            self._store_candle(tf, row)


    async def _pipeline(self, tf, interval):

        interval_seconds = interval * 60

        while self._running:

            cycle_start = time.time()

            try:

                df = self.engine.get_ohlc(
                    exchange=self.exchange,
                    token=self.token,
                    interval=interval
                )

                self._process_response(tf, df)

            except Exception as e:

                print(f"[REST PIPELINE ERROR] {tf} : {e}")

            elapsed = time.time() - cycle_start

            sleep_time = interval_seconds - elapsed

            if sleep_time > 0:

                await asyncio.sleep(sleep_time)

            else:

                await asyncio.sleep(1)


    async def start(self):

        if self._running:

            return

        self._running = True

        loop = asyncio.get_running_loop()

        self._tasks.append(loop.create_task(self._pipeline("1m", 1)))
        self._tasks.append(loop.create_task(self._pipeline("3m", 3)))
        self._tasks.append(loop.create_task(self._pipeline("5m", 5)))


    async def stop(self):

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

    def __init__(self, engine, exchange, token):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        self.tick_agg = TickCandleAggregator()

        self.rest_agg = RestCandleAggregator(
            engine,
            exchange,
            token
        )


    async def start(self):

        await self.rest_agg.start()


    async def stop(self):

        await self.rest_agg.stop()


    def update_tick(self, price):

        self.tick_agg.update_tick(price)


    def get(self, timeframe):

        if timeframe in ["10s", "15s", "30s"]:

            return self.tick_agg.get(timeframe)

        elif timeframe in ["1m", "3m", "5m"]:

            return self.rest_agg.get(timeframe)

        else:

            return None
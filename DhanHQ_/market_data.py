# ============================================================
# MARKET DATA MANAGER v4.2
# Clock Barrier Synchronized
# Production Grade
# ============================================================

import time
import threading
from datetime import datetime, timedelta
import pandas as pd

from collections import deque
from queue import Queue, Empty


# ============================================================
# GLOBAL CLOCK BARRIER
# ============================================================

class ClockBarrier:

    def __init__(self):

        self._event = threading.Event()
        self._lock = threading.Lock()
        self.last_pulse = None


    def pulse(self, timestamp):

        with self._lock:

            if self.last_pulse == timestamp:
                return

            self.last_pulse = timestamp

            self._event.set()
            self._event.clear()


    def wait(self, timeout=None):

        self._event.wait(timeout)


ENGINE_CLOCK = ClockBarrier()


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

        self._lock = threading.Lock()


    def append(self, o, h, l, c, v, t):

        with self._lock:

            if self.time and t <= self.time[-1]:
                return

            self.open.append(o)
            self.high.append(h)
            self.low.append(l)
            self.close.append(c)
            self.volume.append(v)
            self.time.append(t)


    def __len__(self):

        return len(self.close)


# ============================================================
# TICK AGGREGATOR
# ============================================================

class TickCandleAggregator:

    def __init__(self, maxlen=500):

        self.timeframes = {"10s":10,"15s":15,"30s":30}

        self.buffers = {}
        self.current = {}

        for tf in self.timeframes:

            self.buffers[tf] = CandleBuffer(maxlen)

            self.current[tf] = {
                "start":None,
                "open":None,
                "high":None,
                "low":None,
                "close":None
            }

        self._lock = threading.Lock()


    def update_tick(self, price, timestamp=None):

        if timestamp is None:
            timestamp = int(time.time())

        with self._lock:

            for tf, seconds in self.timeframes.items():

                bucket = timestamp - (timestamp % seconds)

                candle = self.current[tf]

                if candle["start"] is None:

                    candle["start"] = bucket
                    candle["open"] = price
                    candle["high"] = price
                    candle["low"] = price
                    candle["close"] = price
                    continue

                if bucket == candle["start"]:

                    candle["high"] = max(candle["high"], price)
                    candle["low"] = min(candle["low"], price)
                    candle["close"] = price
                    continue

                if bucket < candle["start"]:
                    continue

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
# REST AGGREGATOR
# ============================================================

class RestCandleAggregator:

    TF_SECONDS = {
        "1m":60,
        "3m":180,
        "5m":300,
        "15m":900,
        "30m":1800,
        "1h":3600
    }


    def __init__(self, engine, exchange, token, required_timeframes):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        self.required_timeframes = set(required_timeframes)

        self.buffers = {}
        self.current = {}

        for tf in self.required_timeframes:

            self.buffers[tf] = CandleBuffer()

            if tf != "1d":

                self.current[tf] = {
                    "start":None,
                    "open":None,
                    "high":None,
                    "low":None,
                    "close":None,
                    "volume":0
                }

        self._last_1m_ts = None

        self._running = False
        self._thread = None

        self._lock = threading.Lock()


    def get(self, tf):

        return self.buffers.get(tf)


    def add_timeframe(self, tf):

        if tf in self.buffers:
            return

        self.buffers[tf] = CandleBuffer()

        if tf != "1d":

            self.current[tf] = {
                "start":None,
                "open":None,
                "high":None,
                "low":None,
                "close":None,
                "volume":0
            }


    def _update_tf(self, tf, seconds, o, h, l, c, v, ts):

        bucket = ts - (ts % seconds)

        candle = self.current[tf]

        if candle["start"] is None:

            candle["start"] = bucket
            candle["open"] = o
            candle["high"] = h
            candle["low"] = l
            candle["close"] = c
            candle["volume"] = v
            return

        if bucket == candle["start"]:

            candle["high"] = max(candle["high"], h)
            candle["low"] = min(candle["low"], l)
            candle["close"] = c
            candle["volume"] += v
            return

        if bucket < candle["start"]:
            return

        self.buffers[tf].append(
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["volume"],
            candle["start"]
        )

        candle["start"] = bucket
        candle["open"] = o
        candle["high"] = h
        candle["low"] = l
        candle["close"] = c
        candle["volume"] = v


    def _process_1m(self, df):

        if df is None or df.empty:
            return

        with self._lock:

            for _, row in df.iterrows():

                ts = int(pd.to_datetime(row["timestamp"]).timestamp())

                if self._last_1m_ts and ts <= self._last_1m_ts:
                    continue

                o = float(row["open"])
                h = float(row["high"])
                l = float(row["low"])
                c = float(row["close"])
                v = float(row["volume"])

                self.buffers["1m"].append(o,h,l,c,v,ts)

                ENGINE_CLOCK.pulse(ts)

                for tf, seconds in self.TF_SECONDS.items():

                    if tf == "1m":
                        continue

                    if tf not in self.current:
                        continue

                    self._update_tf(tf,seconds,o,h,l,c,v,ts)

                self._last_1m_ts = ts


    def _pipeline(self):

        while self._running:

            try:

                start_time = datetime.now() - timedelta(hours=1)

                df = self.engine.get_ohlc(
                    exchange=self.exchange,
                    token=self.token,
                    start=start_time,
                    interval="min"
                )

                self._process_1m(df)

            except Exception as e:

                print("[REST PIPELINE ERROR]", e)

            time.sleep(60)


    def start(self):

        if self._running:
            return

        self._running = True

        self._thread = threading.Thread(
            target=self._pipeline,
            daemon=True
        )

        self._thread.start()


    def stop(self):

        self._running = False


# ============================================================
# MARKET DATA MANAGER
# ============================================================

class MarketDataManager:

    def __init__(self, engine, exchange, token, required_timeframes):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        key = f"{exchange}|{token}"

        self.engine.market_data_map[key] = self

        self.tick_queue = Queue(maxsize=2000)

        self.tick_agg = TickCandleAggregator()

        self.rest_agg = RestCandleAggregator(
            engine,
            exchange,
            token,
            required_timeframes
        )

        self._running = False
        self._tick_thread = None


    def on_tick(self, price):

        try:
            self.tick_queue.put_nowait(price)
        except:
            pass


    def _tick_worker(self):

        while self._running:

            try:

                price = self.tick_queue.get(timeout=1)

                self.tick_agg.update_tick(price)

            except Empty:
                continue


    def start(self):

        if self._running:
            return

        self._running = True

        self.rest_agg.start()

        self._tick_thread = threading.Thread(
            target=self._tick_worker,
            daemon=True
        )

        self._tick_thread.start()


    def stop(self):

        self._running = False

        self.rest_agg.stop()


    def get(self, timeframe):

        if timeframe in ["10s","15s","30s"]:
            return self.tick_agg.get(timeframe)

        return self.rest_agg.get(timeframe)

    




#_#_#
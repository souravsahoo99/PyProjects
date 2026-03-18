# ============================================================
# MARKET DATA MANAGER v3.3
# Production Grade
# DefineEdge Compatible
# Deterministic Candle Aggregation
# REST 1D Fetching Supported
# ============================================================

import time
import asyncio
import threading
from datetime import datetime ,timedelta
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

        if self.time and t <= self.time[-1]:
            return

        self.open.append(o)
        self.high.append(h)
        self.low.append(l)
        self.close.append(c)
        self.volume.append(v)
        self.time.append(t)

    def last(self):

        if not self.close:
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
# TICK AGGREGATOR
# ============================================================

class TickCandleAggregator:

    def __init__(self, maxlen=500):

        self.timeframes = {"10s":10,"15s":15,"30s":30}

        self.buffers={}
        self.current={}

        for tf in self.timeframes:

            self.buffers[tf]=CandleBuffer(maxlen)

            self.current[tf]={
                "start":None,
                "open":None,
                "high":None,
                "low":None,
                "close":None
            }

        self._lock=threading.Lock()

    def update_tick(self,price,timestamp=None):

        if timestamp is None:
            timestamp=int(time.time())

        with self._lock:

            for tf,seconds in self.timeframes.items():

                bucket=timestamp-(timestamp%seconds)

                candle=self.current[tf]

                if candle["start"] is None:

                    candle["start"]=bucket
                    candle["open"]=price
                    candle["high"]=price
                    candle["low"]=price
                    candle["close"]=price
                    continue

                if bucket==candle["start"]:

                    candle["high"]=max(candle["high"],price)
                    candle["low"]=min(candle["low"],price)
                    candle["close"]=price
                    continue

                if bucket<candle["start"]:
                    continue

                self.buffers[tf].append(
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    0,
                    candle["start"]
                )

                candle["start"]=bucket
                candle["open"]=price
                candle["high"]=price
                candle["low"]=price
                candle["close"]=price

    def get(self,tf):
        return self.buffers.get(tf)


# ============================================================
# REST AGGREGATOR
# ============================================================

class RestCandleAggregator:

    TF_SECONDS={
        "1m":60,
        "3m":180,
        "5m":300,
        "15m":900,
        "30m":1800,
        "1h":3600
    }

    def __init__(self,engine,exchange,token,required_timeframes=None,maxlen=500):

        self.engine=engine
        self.exchange=exchange
        self.token=token

        if required_timeframes is None:
            required_timeframes=["1m"]

        self.required_timeframes=set(required_timeframes)

        self.buffers={}
        self.current={}

        for tf in self.required_timeframes:

            self.buffers[tf]=CandleBuffer(maxlen)

            if tf!="1d":
                self.current[tf]={
                    "start":None,
                    "open":None,
                    "high":None,
                    "low":None,
                    "close":None,
                    "volume":0
                }

        self._last_1m_ts=None
        self._running=False
        self._tasks=[]

    # ========================================================
    # ADD TIMEFRAME
    # ========================================================

    def add_timeframe(self,tf):

        if tf in self.buffers:
            return

        self.buffers[tf]=CandleBuffer()

        if tf!="1d":

            self.current[tf]={
                "start":None,
                "open":None,
                "high":None,
                "low":None,
                "close":None,
                "volume":0
            }

        self.required_timeframes.add(tf)

    # ========================================================
    # AGGREGATE TF
    # ========================================================

    def _update_tf(self,tf,seconds,o,h,l,c,v,ts):

        bucket=ts-(ts%seconds)

        candle=self.current[tf]

        if candle["start"] is None:

            candle["start"]=bucket
            candle["open"]=o
            candle["high"]=h
            candle["low"]=l
            candle["close"]=c
            candle["volume"]=v
            return

        if bucket==candle["start"]:

            candle["high"]=max(candle["high"],h)
            candle["low"]=min(candle["low"],l)
            candle["close"]=c
            candle["volume"]+=v
            return

        if bucket<candle["start"]:
            return

        self.buffers[tf].append(
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["volume"],
            candle["start"]
        )

        candle["start"]=bucket
        candle["open"]=o
        candle["high"]=h
        candle["low"]=l
        candle["close"]=c
        candle["volume"]=v

    # ========================================================
    # PROCESS 1M
    # ========================================================

    def _process_1m(self,df):

        if df is None or df.empty:
            return

        for _,row in df.iterrows():

            ts=int(pd.to_datetime(row["timestamp"]).timestamp())

            if self._last_1m_ts and ts<=self._last_1m_ts:
                continue

            if self._last_1m_ts:

                expected=self._last_1m_ts+60

                while ts>expected:

                    last=self.buffers["1m"].last()

                    if last:

                        self.buffers["1m"].append(
                            last["close"],
                            last["close"],
                            last["close"],
                            last["close"],
                            0,
                            expected
                        )

                    expected+=60

            o=float(row["open"])
            h=float(row["high"])
            l=float(row["low"])
            c=float(row["close"])
            v=float(row["volume"])

            if "1m" in self.buffers:
                self.buffers["1m"].append(o,h,l,c,v,ts)

            for tf,seconds in self.TF_SECONDS.items():

                if tf=="1m":
                    continue

                if tf not in self.current:
                    continue

                self._update_tf(tf,seconds,o,h,l,c,v,ts)

            self._last_1m_ts=ts

    # ========================================================
    # 1 MIN PIPELINE
    # ========================================================

    async def _minute_pipeline(self):

        while self._running:

            try:

                df=self.engine.get_ohlc(
                    exchange=self.exchange,
                    token=self.token,
                    interval="min"
                )

                self._process_1m(df)

            except Exception as e:
                print("[REST 1M ERROR]",e)

            sleep=60-(time.time()%60)

            await asyncio.sleep(sleep)

    # ========================================================
    # DAILY FETCH FUNCTION
    # ========================================================

    def fetch_daily_candle(self):
        
        today = datetime.today()
        days_ago = datetime.now() - timedelta(days=150)
        start_date = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)

        try:

            raw=self.engine.api.Get_Daily_Data(
                trading_symbol=self.token,
                exchange=self.exchange,
                start=start_date,
                end=today
            )

            df=pd.DataFrame(list(raw))

            if df.empty:
                return

            df=df.rename(columns={"datetime":"timestamp"})

            for _,row in df.iterrows():

                ts=int(pd.to_datetime(row["timestamp"]).timestamp())

                self.buffers["1d"].append(
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["volume"]),
                    ts
                )

        except Exception as e:
            print("[DAILY FETCH ERROR]",e)

    # ========================================================
    # DAILY PIPELINE
    # ========================================================

    async def _daily_pipeline(self):

        while self._running:

            self.fetch_daily_candle()

            await asyncio.sleep(86400)

    # ========================================================
    # START
    # ========================================================

    async def start(self):

        if self._running:
            return

        self._running=True

        loop=asyncio.get_running_loop()

        self._tasks.append(loop.create_task(self._minute_pipeline()))

        if "1d" in self.required_timeframes:
            self._tasks.append(loop.create_task(self._daily_pipeline()))

    async def stop(self):

        self._running=False

        for t in self._tasks:
            t.cancel()

        for t in self._tasks:
            try:
                await t
            except:
                pass

    def get(self,tf):
        return self.buffers.get(tf)


# ============================================================
# MARKET DATA MANAGER
# ============================================================

class MarketDataManager:

    def __init__(self,engine,exchange,token,required_timeframes=None):

        self.engine=engine
        self.exchange=exchange
        self.token=token

        if required_timeframes is None:
            required_timeframes=["1m"]

        key=f"{exchange}|{token}"

        if key not in self.engine.market_data_map:
            self.engine.market_data_map[key]=self

        self.tick_queue=asyncio.Queue(maxsize=2000)

        self.tick_agg=TickCandleAggregator()

        self.rest_agg=RestCandleAggregator(
            engine,
            exchange,
            token,
            required_timeframes
        )

        self._tick_task=None
        self._running=False

    def ensure_timeframe(self,tf):

        if tf in ["10s","15s","30s"]:
            return

        if tf not in self.rest_agg.buffers:
            self.rest_agg.add_timeframe(tf)

    async def _tick_worker(self):

        while self._running:

            try:

                price=await asyncio.wait_for(self.tick_queue.get(),timeout=1)

                if price is None:
                    continue

                self.tick_agg.update_tick(price)

            except asyncio.TimeoutError:
                continue

    async def start(self):

        if self._running:
            return

        self._running=True

        await self.rest_agg.start()

        loop=asyncio.get_running_loop()

        self._tick_task=loop.create_task(self._tick_worker())

    async def stop(self):

        self._running=False

        await self.rest_agg.stop()

        if self._tick_task:
            self._tick_task.cancel()

        key=f"{self.exchange}|{self.token}"

        if key in self.engine.market_data_map:
            del self.engine.market_data_map[key]

    def get(self,timeframe):

        if timeframe in ["10s","15s","30s"]:
            return self.tick_agg.get(timeframe)

        return self.rest_agg.get(timeframe)
    




#_#
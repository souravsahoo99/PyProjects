# ============================================================
# CANDLE CHART   v1.1
# Lightweight TradingView Chart for Debug Visualization
# ============================================================

"""
candle_chart.py

Purpose
-------
Live candlestick visualization module for debugging and analysis.

Architecture
------------
ShoonyaEngine
      │
      ▼
MarketDataManager
      │
      ▼
CandleBuffer
      │
      ▼
Lightweight Charts

Supported Timeframes
--------------------
10s, 15s, 30s
1m, 3m, 5m, 30m
1D
"""

import asyncio
import pandas as pd

from lightweight_charts import Chart
from market_data import MarketDataManager


# ============================================================
# SUPPORTED TIMEFRAMES
# ============================================================

SUPPORTED_TIMEFRAMES = {

    "10s",
    "15s",
    "30s",

    "1m",
    "3m",
    "5m",
    "30m",

    "1D"

}


# ============================================================
# CANDLE CHART CLASS
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.symbol = None
        self.exchange = None
        self.token = None

        self.timeframe = "1m"

        self.market_data = None
        self.chart = None

        self._running = False
        self._task = None


    # ========================================================
    # SYMBOL RESOLUTION
    # ========================================================

    def _resolve_symbol(self, symbol, exchange):

        token = self.registry.get_token(exchange, symbol)

        if token is None:
            raise Exception(f"[CHART] Symbol not found → {symbol}")

        self.symbol = symbol
        self.exchange = exchange
        self.token = token

        print(f"[CHART] Resolved {symbol} → {exchange}|{token}")


    # ========================================================
    # WEBSOCKET SUBSCRIPTION CHECK
    # ========================================================

    def _ensure_subscription(self):

        instrument = f"{self.exchange}|{self.token}"

        if instrument not in self.engine._subscribed_instruments:

            print(f"[CHART] Subscribing {instrument}")

            self.engine.subscribe(self.exchange, self.token)

        else:

            print(f"[CHART] Using existing subscription {instrument}")


    # ========================================================
    # BUILD DATA PIPELINE
    # ========================================================

    async def _build_pipeline(self):

        self.market_data = MarketDataManager(

            self.engine,
            self.exchange,
            self.token

        )

        await self.market_data.start()

        print("[CHART] Market data pipeline started")


    # ========================================================
    # CREATE CHART WINDOW
    # ========================================================

    def _create_chart(self):

        self.chart = Chart()

        self.chart.layout(

            background_color="#0f172a",
            text_color="#e2e8f0"

        )

        self.chart.grid(

            vert_enabled=True,
            horz_enabled=True

        )

        self.chart.legend(True)

        print("[CHART] Chart window created")


    # ========================================================
    # BUILD DATAFRAME
    # ========================================================

    def _build_dataframe(self, buffer):

        if buffer is None or len(buffer) == 0:
            return None

        df = pd.DataFrame({

            "time": list(buffer.time),
            "open": list(buffer.open),
            "high": list(buffer.high),
            "low": list(buffer.low),
            "close": list(buffer.close)

        })

        df["time"] = pd.to_datetime(df["time"], unit="s")

        return df


    # ========================================================
    # INITIAL LOAD
    # ========================================================

    def _load_initial_candles(self):

        buffer = self.market_data.get(self.timeframe)

        df = self._build_dataframe(buffer)

        if df is None:
            print("[CHART] Waiting for candle data...")
            return

        self.chart.set(df)

        print("[CHART] Initial candles loaded")


    # ========================================================
    # LIVE UPDATE LOOP
    # ========================================================

    async def _update_loop(self):

        last_candle_time = None

        self._running = True

        while self._running:

            buffer = self.market_data.get(self.timeframe)

            if buffer is None or len(buffer) == 0:

                await asyncio.sleep(0.2)
                continue

            candle_time = buffer.time[-1]

            if candle_time == last_candle_time:

                await asyncio.sleep(0.1)
                continue

            last_candle_time = candle_time

            candle = {

                "time": pd.to_datetime(candle_time, unit="s"),
                "open": buffer.open[-1],
                "high": buffer.high[-1],
                "low": buffer.low[-1],
                "close": buffer.close[-1]

            }

            self.chart.update(candle)

            await asyncio.sleep(0.05)


    # ========================================================
    # START CHART
    # ========================================================

    async def start(self, symbol, exchange, timeframe="1m"):

        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise Exception(f"[CHART] Unsupported timeframe → {timeframe}")

        self.timeframe = timeframe

        print(f"[CHART] Starting chart → {symbol} | {timeframe}")

        self._resolve_symbol(symbol, exchange)

        self._ensure_subscription()

        await self._build_pipeline()

        self._create_chart()

        await asyncio.sleep(2)

        self._load_initial_candles()

        loop = asyncio.get_running_loop()

        self._task = loop.create_task(self._update_loop())


    # ========================================================
    # STOP CHART
    # ========================================================

    async def stop(self):

        print("[CHART] Stopping chart")

        self._running = False

        if self.market_data:

            await self.market_data.stop()

        if self._task:

            self._task.cancel()




#_
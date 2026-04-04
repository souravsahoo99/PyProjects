# ============================================================
# SIMPLE CANDLE CHART v5.0
# Single Pane Debug Chart
# Threaded Runtime Version
# Fully Compatible with Engine Architecture
# ============================================================

import time
import threading
import pandas as pd

from lightweight_charts import Chart


# ============================================================
# CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.chart = None

        self.symbol = None
        self.exchange = None
        self.token = None
        self.timeframe = None

        self.market_data = None

        self.chart_initialized = False
        self.last_candle_time = None

        self.price_line = None

        self.refresh_interval = 0.5
        self._running = False

        self._thread = None


    # ========================================================
    # INITIALIZE CHART
    # ========================================================

    def initialize(self, symbol, exchange, timeframe="1m"):

        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe

        # ----------------------------------------------------
        # TOKEN RESOLUTION
        # ----------------------------------------------------

        token = None

        if hasattr(self.registry, "get_security_id"):
            token = self.registry.get_security_id(exchange, symbol)

        if token is None:
            print("[CHART] Symbol not found:", symbol)
            return

        self.token = token

        # ----------------------------------------------------
        # ATTACH EXISTING PIPELINE
        # ----------------------------------------------------

        for md in self.engine.market_data_map.values():

            if md.token == token:
                self.market_data = md
                break

        # ----------------------------------------------------
        # CREATE PIPELINE IF MISSING
        # ----------------------------------------------------

        if self.market_data is None:

            from market_data import MarketDataManager

            print("[CHART] Creating pipeline for", symbol)

            self.market_data = MarketDataManager(
                self.engine,
                exchange,
                token,
                [timeframe]
            )

            self.market_data.start()

        # ----------------------------------------------------
        # ENSURE TIMEFRAME PIPELINE
        # ----------------------------------------------------

        self.market_data.ensure_timeframe(timeframe)

        # ----------------------------------------------------
        # CREATE CHART
        # ----------------------------------------------------

        self.chart = Chart()

        self.chart.legend(True)

        self.price_line = self.chart.create_line("Price")

        # ----------------------------------------------------
        # START THREAD LOOP
        # ----------------------------------------------------

        self._running = True

        self._thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )

        self._thread.start()


    # ========================================================
    # BUFFER → DATAFRAME
    # ========================================================

    def _build_dataframe(self, buffer):

        if buffer is None or len(buffer.time) == 0:
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
    # DRAW CANDLES
    # ========================================================

    def draw_candles(self):

        if self.market_data is None:
            return

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer.time) == 0:
            return

        df = self._build_dataframe(buffer)

        if df is None:
            return

        # ----------------------------------------------------
        # INITIAL CHART LOAD
        # ----------------------------------------------------

        if not self.chart_initialized:

            self.chart.set(df)

            self.chart_initialized = True
            self.last_candle_time = buffer.time[-1]

            return

        # ----------------------------------------------------
        # UPDATE ONLY NEW CANDLE
        # ----------------------------------------------------

        candle_time = buffer.time[-1]

        if candle_time == self.last_candle_time:
            return

        self.last_candle_time = candle_time

        last_row = df.iloc[-1]

        self.chart.update({

            "time": last_row["time"],
            "open": last_row["open"],
            "high": last_row["high"],
            "low": last_row["low"],
            "close": last_row["close"]

        })


    # ========================================================
    # LIVE PRICE
    # ========================================================

    def draw_price(self):

        if self.engine is None:
            return

        price = self.engine.get_ltp_live(self.exchange, self.token)

        if price is None:
            return

        self.price_line.update(price)


    # ========================================================
    # UPDATE LOOP (THREAD)
    # ========================================================

    def _update_loop(self):

        while self._running:

            try:

                self.draw_candles()
                self.draw_price()

            except Exception as e:

                print("[CHART ERROR]", e)

            time.sleep(self.refresh_interval)


    # ========================================================
    # STOP CHART
    # ========================================================

    def stop(self):

        self._running = False

        if self._thread:
            self._thread.join(timeout=1)

        print("[CHART] Stopped")



#
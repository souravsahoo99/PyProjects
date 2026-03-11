# ============================================================
# CANDLE CHART
# Lightweight Debug Chart
# ============================================================

import asyncio
import pandas as pd
from lightweight_charts import Chart


# ============================================================
# CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.symbol = None
        self.exchange = None
        self.token = None
        self.timeframe = None

        self.market_data = None
        self.signal_engine = None

        self.chart = None

        self.last_candle_time = None
        self.last_signal_index = 0

        self.refresh_interval = 0.36


    # ========================================================
    # START CHART
    # ========================================================

    async def start(self, symbol, exchange, timeframe):

        print(f"[CHART] Starting chart for {symbol}")

        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe

        token = self.registry.get_token(exchange, symbol)

        if token is None:

            print("[CHART] Symbol not found in registry")
            return

        self.token = token

        # ----------------------------------------------------
        # Find existing node (if charted symbol already used)
        # ----------------------------------------------------

        self.market_data = None
        self.signal_engine = None

        for md in self.engine.market_data_map.values():

            if md.token == token:

                self.market_data = md
                break

        # ----------------------------------------------------
        # If node doesn't exist → create temporary pipeline
        # ----------------------------------------------------

        if self.market_data is None:

            from market_data import MarketDataManager

            print("[CHART] Creating standalone market data pipeline")

            self.market_data = MarketDataManager(
                self.engine,
                exchange,
                token
            )

            await self.market_data.start()

        # ----------------------------------------------------
        # Locate signal engine
        # ----------------------------------------------------

        if hasattr(self.market_data, "signal_engine"):
            self.signal_engine = self.market_data.signal_engine

        # ----------------------------------------------------
        # Create chart window
        # ----------------------------------------------------

        self.chart = Chart()

        self.chart.legend(True)

        # ----------------------------------------------------
        # Start update loop
        # ----------------------------------------------------

        asyncio.create_task(self._update_loop())


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
    # DRAW CANDLES
    # ========================================================

    def _draw_candles(self):

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) == 0:
            return

        candle_time = buffer.time[-1]

        if candle_time == self.last_candle_time:
            return

        self.last_candle_time = candle_time

        df = self._build_dataframe(buffer)

        if df is None:
            return

        self.chart.set(df)


    # ========================================================
    # DRAW SIGNAL MARKERS
    # ========================================================

    def _draw_signals(self):

        if self.signal_engine is None:
            return

        signals = self.signal_engine.signal_history

        if signals is None:
            return

        while self.last_signal_index < len(signals):

            signal = signals[self.last_signal_index]

            self.last_signal_index += 1

            ts = pd.to_datetime(signal["signal_time"], unit="s")

            side = signal["side"]

            price = signal["entry_price"]

            if side == "BUY":

                self.chart.marker(

                    time=ts,
                    position="belowBar",
                    shape="arrowUp",
                    color="green",
                    text="BUY"

                )

            elif side == "SELL":

                self.chart.marker(

                    time=ts,
                    position="aboveBar",
                    shape="arrowDown",
                    color="red",
                    text="SELL"

                )


    # ========================================================
    # UPDATE LOOP
    # ========================================================

    async def _update_loop(self):

        while True:

            try:

                self._draw_candles()

                self._draw_signals()

            except Exception as e:

                print("[CHART ERROR]", e)

            await asyncio.sleep(self.refresh_interval)












# ============================================================
# CANDLE CHART
# Lightweight Debug Chart
# ============================================================

import asyncio
import pandas as pd
from lightweight_charts import Chart


# ============================================================
# CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.symbol = None
        self.exchange = None
        self.token = None
        self.timeframe = None

        self.market_data = None
        self.signal_engine = None

        self.chart = None

        self.last_candle_time = None
        self.last_signal_index = 0

        self.refresh_interval = 0.36


    # ========================================================
    # START CHART
    # ========================================================

    async def start(self, symbol, exchange, timeframe):

        print(f"[CHART] Starting chart for {symbol}")

        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe

        token = self.registry.get_token(exchange, symbol)

        if token is None:

            print("[CHART] Symbol not found in registry")
            return

        self.token = token

        # ----------------------------------------------------
        # Find existing node (if charted symbol already used)
        # ----------------------------------------------------

        self.market_data = None
        self.signal_engine = None

        for md in self.engine.market_data_map.values():

            if md.token == token:

                self.market_data = md
                break

        # ----------------------------------------------------
        # If node doesn't exist → create temporary pipeline
        # ----------------------------------------------------

        if self.market_data is None:

            from market_data import MarketDataManager

            print("[CHART] Creating standalone market data pipeline")

            self.market_data = MarketDataManager(
                self.engine,
                exchange,
                token
            )

            await self.market_data.start()

        # ----------------------------------------------------
        # Locate signal engine
        # ----------------------------------------------------

        if hasattr(self.market_data, "signal_engine"):
            self.signal_engine = self.market_data.signal_engine

        # ----------------------------------------------------
        # Create chart window
        # ----------------------------------------------------

        self.chart = Chart()

        self.chart.legend(True)

        # ----------------------------------------------------
        # Start update loop
        # ----------------------------------------------------

        asyncio.create_task(self._update_loop())


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
    # DRAW CANDLES
    # ========================================================

    def _draw_candles(self):

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) == 0:
            return

        candle_time = buffer.time[-1]

        if candle_time == self.last_candle_time:
            return

        self.last_candle_time = candle_time

        df = self._build_dataframe(buffer)

        if df is None:
            return

        self.chart.set(df)


    # ========================================================
    # DRAW SIGNAL MARKERS
    # ========================================================

    def _draw_signals(self):

        if self.signal_engine is None:
            return

        signals = self.signal_engine.signal_history

        if signals is None:
            return

        while self.last_signal_index < len(signals):

            signal = signals[self.last_signal_index]

            self.last_signal_index += 1

            ts = pd.to_datetime(signal["signal_time"], unit="s")

            side = signal["side"]

            price = signal["entry_price"]

            if side == "BUY":

                self.chart.marker(

                    time=ts,
                    position="belowBar",
                    shape="arrowUp",
                    color="green",
                    text="BUY"

                )

            elif side == "SELL":

                self.chart.marker(

                    time=ts,
                    position="aboveBar",
                    shape="arrowDown",
                    color="red",
                    text="SELL"

                )


    # ========================================================
    # UPDATE LOOP
    # ========================================================

    async def _update_loop(self):

        while True:

            try:

                self._draw_candles()

                self._draw_signals()

            except Exception as e:

                print("[CHART ERROR]", e)

            await asyncio.sleep(self.refresh_interval)



            
#_#_#_#_#_#_#_#_#_
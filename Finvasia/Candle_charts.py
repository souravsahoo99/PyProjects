# ============================================================
# CANDLE CHART
# Lightweight Debug Chart (Production Version)
# Base: Checkpoint 5.0
# ============================================================

import asyncio
import pandas as pd
from lightweight_charts import Chart

from instrument_node import InstrumentNode


# ============================================================
# CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.node = None
        self.buffer = None

        self.symbol = None
        self.exchange = None
        self.token = None

        self.timeframe = "1m"

        self.chart = None

        self._last_candle_time = None
        self._last_price = None

        # refresh interval
        self.refresh_interval = 0.36


    # ========================================================
    # RESOLVE SYMBOL
    # ========================================================

    def _resolve_symbol(self, symbol):

        token = None
        exchange = None

        for (ex, sym), tk in self.registry.symbol_map.items():

            if sym == symbol:
                token = tk
                exchange = ex
                break

        if token is None:
            return None, None

        return exchange, token


    # ========================================================
    # FIND EXISTING NODE
    # ========================================================

    def _find_existing_node(self, exchange, token):

        key = f"{exchange}|{token}"

        md = self.engine.market_data_map.get(key)

        if md is None:
            return None

        # create lightweight wrapper node reference
        node = InstrumentNode(self.engine, exchange, "", token)
        node.market_data = md

        return node


    # ========================================================
    # CREATE TEMPORARY NODE
    # ========================================================

    async def _create_temp_node(self, exchange, symbol, token):

        node = InstrumentNode(
            self.engine,
            exchange,
            symbol,
            token
        )

        await node.initialize()

        node.start()

        return node


    # ========================================================
    # ATTACH BUFFER
    # ========================================================

    def _attach_buffer(self):

        if self.node is None:
            return

        self.buffer = self.node.market_data.get(self.timeframe)


    # ========================================================
    # BUILD DATAFRAME
    # ========================================================

    def _build_dataframe(self):

        if self.buffer is None:
            return None

        if len(self.buffer) == 0:
            return None

        df = pd.DataFrame({

            "time": list(self.buffer.time),
            "open": list(self.buffer.open),
            "high": list(self.buffer.high),
            "low": list(self.buffer.low),
            "close": list(self.buffer.close)

        })

        df["time"] = pd.to_datetime(df["time"], unit="s")

        return df


    # ========================================================
    # LOAD INITIAL CANDLES
    # ========================================================

    def _load_initial_candles(self):

        df = self._build_dataframe()

        if df is None:
            return

        self.chart.set(df)


    # ========================================================
    # UPDATE CHART
    # ========================================================

    def _update_chart(self):

        if self.buffer is None:
            return

        if len(self.buffer) == 0:
            return

        candle_time = self.buffer.time[-1]

        # new candle
        if candle_time != self._last_candle_time:

            candle = {

                "time": pd.to_datetime(candle_time, unit="s"),
                "open": self.buffer.open[-1],
                "high": self.buffer.high[-1],
                "low": self.buffer.low[-1],
                "close": self.buffer.close[-1]

            }

            self.chart.update(candle)

            self._last_candle_time = candle_time


    # ========================================================
    # UPDATE PRICE MARKER
    # ========================================================

    def _update_price(self):

        price = self.engine.get_best_ltp(
            self.exchange,
            self.token
        )

        if price is None:
            return

        if price == self._last_price:
            return

        try:

            self.chart.price_line(price)

        except Exception:

            pass

        self._last_price = price


    # ========================================================
    # PARSE LOCAL SIGNALS
    # ========================================================

    def _update_signals(self):

        if self.node is None:
            return

        se = getattr(self.node, "signal_engine", None)

        if se is None:
            return

        signals = getattr(se, "local_signals", None)

        if signals is None:
            return

        if len(signals) == 0:
            return

        signal = signals[-1]

        try:

            marker = {

                "time": pd.to_datetime(signal["signal_time"], unit="s"),
                "position": "belowBar" if signal["side"] == "BUY" else "aboveBar",
                "shape": "arrowUp" if signal["side"] == "BUY" else "arrowDown",
                "color": "#26a69a" if signal["side"] == "BUY" else "#ef5350",
                "text": signal["side"]

            }

            self.chart.marker(marker)

        except Exception:

            pass


    # ========================================================
    # SYMBOL SWITCH
    # ========================================================

    async def switch_symbol(self, symbol):

        exchange, token = self._resolve_symbol(symbol)

        if token is None:
            return

        node = self._find_existing_node(exchange, token)

        if node is None:

            node = await self._create_temp_node(
                exchange,
                symbol,
                token
            )

        self.node = node
        self.symbol = symbol
        self.exchange = exchange
        self.token = token

        self._attach_buffer()

        self._load_initial_candles()


    # ========================================================
    # TIMEFRAME SWITCH
    # ========================================================

    def switch_timeframe(self, timeframe):

        self.timeframe = timeframe

        self._attach_buffer()

        self._load_initial_candles()


    # ========================================================
    # START CHART
    # ========================================================

    async def start(self, symbol, timeframe="1m"):

        self.timeframe = timeframe

        await self.switch_symbol(symbol)

        self.chart = Chart()

        self.chart.layout(
            background="#131722",
            text_color="#d1d4dc"
        )

        self.chart.candle_style(
            up_color="#26a69a",
            down_color="#ef5350"
        )

        self._load_initial_candles()

        asyncio.create_task(self._run_loop())


    # ========================================================
    # RENDER LOOP
    # ========================================================

    async def _run_loop(self):

        while True:

            try:

                self._update_chart()

                self._update_price()

                self._update_signals()

            except Exception:

                pass

            await asyncio.sleep(self.refresh_interval)



#_#_
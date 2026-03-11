# ============================================================
# CANDLE CHART v3.1
# Dual Pane Debug Chart (Checkpoint 2.3 Compatible)
# ============================================================

import asyncio
import pandas as pd

from lightweight_charts import Chart
from indicator_utils import VWAPBands


# ============================================================
# INTERNAL PANE CONTROLLER
# ============================================================

class _ChartPane:

    def __init__(self, chart, engine, registry):

        self.chart = chart
        self.engine = engine
        self.registry = registry

        self.symbol = None
        self.exchange = None
        self.token = None
        self.timeframe = None

        self.market_data = None
        self.signal_engine = None

        self.chart_initialized = False
        self.last_candle_time = None
        self.last_signal_index = 0

        # chart series
        self.price_line = None
        self.vwap_line = None
        self.vwap_upper = None
        self.vwap_lower = None
        self.orb_high_line = None
        self.orb_low_line = None
        self.vah_line = None
        self.val_line = None


    # --------------------------------------------------------
    # INITIALIZE PANE
    # --------------------------------------------------------

    async def initialize(self, symbol, exchange, timeframe):

        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe

        # compatible token lookup
        token = None

        if hasattr(self.registry, "get_token"):
            token = self.registry.get_token(exchange, symbol)

        if token is None and hasattr(self.registry, "get_security_id"):
            token = self.registry.get_security_id(exchange, symbol)

        if token is None:
            print("[CHART] Symbol not found:", symbol)
            return

        self.token = token

        # attach existing market data pipeline
        for md in self.engine.market_data_map.values():

            if md.token == token:

                self.market_data = md
                break

        # create pipeline if missing
        if self.market_data is None:

            from market_data import MarketDataManager

            print("[CHART] Creating pipeline for", symbol)

            self.market_data = MarketDataManager(
                self.engine,
                exchange,
                token
            )

            await self.market_data.start()

        # attach signal engine
        for node in getattr(self.engine, "instrument_nodes", []):

            if node.token == token:

                self.signal_engine = node.signal_engine
                break

        # create chart lines
        self.price_line = self.chart.create_line("Price")

        self.vwap_line = self.chart.create_line("VWAP")
        self.vwap_upper = self.chart.create_line("VWAP Upper1")
        self.vwap_lower = self.chart.create_line("VWAP Lower1")

        self.orb_high_line = self.chart.create_line("ORB High")
        self.orb_low_line = self.chart.create_line("ORB Low")

        self.vah_line = self.chart.create_line("VAH")
        self.val_line = self.chart.create_line("VAL")


    # --------------------------------------------------------
    # DATAFRAME BUILDER
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # DRAW CANDLES
    # --------------------------------------------------------

    def draw_candles(self):

        if self.market_data is None:
            return

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) == 0:
            return

        df = self._build_dataframe(buffer)

        if df is None:
            return

        if not self.chart_initialized:

            self.chart.set(df)

            self.chart_initialized = True
            self.last_candle_time = buffer.time[-1]

            return

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


    # --------------------------------------------------------
    # LIVE PRICE
    # --------------------------------------------------------

    def draw_price(self):

        price = self.engine.get_ltp_live(self.exchange, self.token)

        if price is None:
            return

        self.price_line.update(price)


    # --------------------------------------------------------
    # VWAP
    # --------------------------------------------------------

    def draw_vwap(self):

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) < 20:
            return

        df = self._build_dataframe(buffer)

        bands = VWAPBands(df).calculate()

        if bands:

            if bands.get("vwap"):
                self.vwap_line.update(bands["vwap"])

            if bands.get("upper1"):
                self.vwap_upper.update(bands["upper1"])

            if bands.get("lower1"):
                self.vwap_lower.update(bands["lower1"])


    # --------------------------------------------------------
    # ORB LEVELS
    # --------------------------------------------------------

    def draw_orb(self):

        if self.signal_engine is None:
            return

        high, low = self.signal_engine.get_orb_levels()

        if high:
            self.orb_high_line.update(high)

        if low:
            self.orb_low_line.update(low)


    # --------------------------------------------------------
    # MARKET PROFILE
    # --------------------------------------------------------

    def draw_profile(self):

        if self.signal_engine is None:
            return

        vah, val = self.signal_engine.get_profile_levels()

        if vah:
            self.vah_line.update(vah)

        if val:
            self.val_line.update(val)


    # --------------------------------------------------------
    # SIGNAL MARKERS
    # --------------------------------------------------------

    def draw_signals(self):

        if self.signal_engine is None:
            return

        signals = self.signal_engine.get_signal_history()

        while self.last_signal_index < len(signals):

            signal = signals[self.last_signal_index]
            self.last_signal_index += 1

            ts = pd.to_datetime(signal["signal_time"], unit="s")

            if signal["side"] == "BUY":

                self.chart.marker(
                    time=ts,
                    position="belowBar",
                    shape="arrowUp",
                    color="green",
                    text="BUY"
                )

            else:

                self.chart.marker(
                    time=ts,
                    position="aboveBar",
                    shape="arrowDown",
                    color="red",
                    text="SELL"
                )


# ============================================================
# MAIN CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        self.engine = engine
        self.registry = registry

        self.chart = None

        self.parent_pane = None
        self.child_pane = None

        self.refresh_interval = 0.36
        self._running = True


    # --------------------------------------------------------
    # START
    # --------------------------------------------------------

    async def start(self, parent_symbol, exchange, timeframe="1m"):

        print("[CHART] Starting dual pane chart")

        self.chart = Chart()
        self.chart.legend(True)

        parent_chart = self.chart.create_subchart(height=0.4)
        child_chart = self.chart.create_subchart(height=0.6)

        self.parent_pane = _ChartPane(parent_chart, self.engine, self.registry)
        self.child_pane = _ChartPane(child_chart, self.engine, self.registry)

        await self.parent_pane.initialize(parent_symbol, exchange, "1m")

        ce_symbol = parent_symbol + "CE"

        await self.child_pane.initialize(ce_symbol, "NFO", "15s")

        asyncio.create_task(self._update_loop())


    # --------------------------------------------------------
    # UPDATE LOOP
    # --------------------------------------------------------

    async def _update_loop(self):

        while self._running:

            try:

                for pane in (self.parent_pane, self.child_pane):

                    if pane is None:
                        continue

                    pane.draw_candles()
                    pane.draw_price()
                    pane.draw_vwap()
                    pane.draw_orb()
                    pane.draw_profile()
                    pane.draw_signals()

            except Exception as e:

                print("[CHART ERROR]", e)

            await asyncio.sleep(self.refresh_interval)





#_#_#_#_#_#_
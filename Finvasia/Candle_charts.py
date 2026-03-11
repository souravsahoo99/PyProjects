# ============================================================
# CANDLE CHART   v2.2
# Production Debug Chart (Smooth Updates)
# ============================================================

import asyncio
import pandas as pd

from lightweight_charts import Chart
from indicator_utils import VWAPBands


# ============================================================
# CANDLE CHART
# ============================================================

class CandleChart:

    def __init__(self, engine, registry):

        # ----------------------------------------------------
        # CORE REFERENCES
        # ----------------------------------------------------

        self.engine = engine
        self.registry = registry

        self.symbol = None
        self.exchange = None
        self.token = None
        self.timeframe = None

        self.market_data = None
        self.signal_engine = None

        self.chart = None

        # ----------------------------------------------------
        # STATE TRACKING
        # ----------------------------------------------------

        self.chart_initialized = False
        self.last_candle_time = None
        self.last_signal_index = 0

        self.refresh_interval = 0.36

        # ----------------------------------------------------
        # SERIES REFERENCES
        # ----------------------------------------------------

        self.price_line = None

        self.vwap_line = None
        self.vwap_upper = None
        self.vwap_lower = None

        self.orb_high_line = None
        self.orb_low_line = None

        self.vah_line = None
        self.val_line = None


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
        # ATTACH EXISTING DATA PIPELINE
        # ----------------------------------------------------

        for md in self.engine.market_data_map.values():

            if md.token == token:

                self.market_data = md
                break

        # ----------------------------------------------------
        # CREATE PIPELINE IF NOT FOUND
        # ----------------------------------------------------

        if self.market_data is None:

            from market_data import MarketDataManager

            print("[CHART] Creating standalone market pipeline")

            self.market_data = MarketDataManager(
                self.engine,
                exchange,
                token
            )

            await self.market_data.start()

        # ----------------------------------------------------
        # ATTACH SIGNAL ENGINE
        # ----------------------------------------------------

        if hasattr(self.market_data, "signal_engine"):

            self.signal_engine = self.market_data.signal_engine

        # ----------------------------------------------------
        # CREATE CHART WINDOW
        # ----------------------------------------------------

        self.chart = Chart()

        self.chart.legend(True)

        # ----------------------------------------------------
        # CREATE LINES
        # ----------------------------------------------------

        self.price_line = self.chart.create_line("Price")

        self.vwap_line = self.chart.create_line("VWAP")
        self.vwap_upper = self.chart.create_line("VWAP Upper1")
        self.vwap_lower = self.chart.create_line("VWAP Lower1")

        self.orb_high_line = self.chart.create_line("ORB High")
        self.orb_low_line = self.chart.create_line("ORB Low")

        self.vah_line = self.chart.create_line("VAH")
        self.val_line = self.chart.create_line("VAL")

        # ----------------------------------------------------
        # START UPDATE LOOP
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
    # DRAW / UPDATE CANDLES
    # ========================================================

    def _draw_candles(self):

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) == 0:
            return

        df = self._build_dataframe(buffer)

        if df is None:
            return

        # ----------------------------------------------------
        # INITIAL LOAD
        # ----------------------------------------------------

        if not self.chart_initialized:

            self.chart.set(df)

            self.chart_initialized = True

            self.last_candle_time = buffer.time[-1]

            return

        # ----------------------------------------------------
        # UPDATE LAST CANDLE
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
    # LIVE PRICE LINE
    # ========================================================

    def _draw_price(self):

        price = self.engine.get_ltp_live(self.exchange, self.token)

        if price is None:
            return

        self.price_line.update(price)


    # ========================================================
    # VWAP BANDS
    # ========================================================

    def _draw_vwap(self):

        buffer = self.market_data.get(self.timeframe)

        if buffer is None or len(buffer) < 20:
            return

        df = self._build_dataframe(buffer)

        if df is None:
            return

        vwap = VWAPBands(df)

        bands = vwap.calculate()

        if bands is None:
            return

        vwap_val = bands.get("vwap")
        upper = bands.get("upper1")
        lower = bands.get("lower1")

        if vwap_val is not None:
            self.vwap_line.update(vwap_val)

        if upper is not None:
            self.vwap_upper.update(upper)

        if lower is not None:
            self.vwap_lower.update(lower)


    # ========================================================
    # ORB LEVELS
    # ========================================================

    def _draw_orb(self):

        if self.signal_engine is None:
            return

        orb_high, orb_low = self.signal_engine.get_orb_levels()

        if orb_high is not None:
            self.orb_high_line.update(orb_high)

        if orb_low is not None:
            self.orb_low_line.update(orb_low)


    # ========================================================
    # MARKET PROFILE LEVELS
    # ========================================================

    def _draw_profile(self):

        if self.signal_engine is None:
            return

        vah, val = self.signal_engine.get_profile_levels()

        if vah is not None:
            self.vah_line.update(vah)

        if val is not None:
            self.val_line.update(val)


    # ========================================================
    # SIGNAL MARKERS
    # ========================================================

    def _draw_signals(self):

        if self.signal_engine is None:
            return

        signals = self.signal_engine.get_signal_history()

        if signals is None:
            return

        while self.last_signal_index < len(signals):

            signal = signals[self.last_signal_index]

            self.last_signal_index += 1

            ts = pd.to_datetime(signal["signal_time"], unit="s")

            side = signal["side"]

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

                self._draw_price()

                self._draw_vwap()

                self._draw_orb()

                self._draw_profile()

                self._draw_signals()

            except Exception as e:

                print("[CHART ERROR]", e)

            await asyncio.sleep(self.refresh_interval)





#_#_#_#_
# ============================================================
# INDICATOR UTILITIES v2.0
# Core Indicator Engines
# Production Safe Version
# ============================================================

import numpy as np
import pandas as pd
from datetime import datetime
import pytz


# ============================================================
# COMMON UTILITIES
# ============================================================

def _resolve_time_column(df):

    if "datetime" in df.columns:
        return "datetime"

    if "timestamp" in df.columns:
        return "timestamp"

    return None


# ============================================================
# OPENING RANGE INDICATOR
# ============================================================

class OpeningRangeIndicator:

    IST = pytz.timezone("Asia/Kolkata")

    HOLIDAYS = [
        "2026-01-26",
        "2026-08-15",
        "2026-10-02",
    ]

    PERCENT_THRESHOLD = 0.004
    FIXED_THRESHOLD = 100
    MAX_RANGE = 600


    def __init__(self, timeframe, instrument, candle_buffer):

        self.timeframe = timeframe
        self.instrument = instrument
        self.candle_buffer = candle_buffer

        self.session_date = None
        self.cached_orb = None

        self.prevDayClose = None
        self.rangeTooBig = False


    # --------------------------------------------------------
    # TRADING DAY VALIDATION
    # --------------------------------------------------------

    def is_trading_day(self):

        now = datetime.now(self.IST)

        if now.weekday() == 6:
            return False

        if now.strftime("%Y-%m-%d") in self.HOLIDAYS:
            return False

        return True


    # --------------------------------------------------------
    # FIRST CANDLE
    # --------------------------------------------------------

    def get_first_candle(self):

        df = self.candle_buffer

        if df is None or len(df) < 2:
            return None

        tcol = _resolve_time_column(df)
        if tcol is None:
            return None

        current = df.iloc[-1]
        previous = df.iloc[-2]

        if current[tcol].date() != previous[tcol].date():
            return current

        return None


    # --------------------------------------------------------
    # LONG CANDLE CHECK
    # --------------------------------------------------------

    def is_long_candle(self, high, low):

        candle_range = high - low
        halfback = (high + low) / 2

        percent_threshold = halfback * self.PERCENT_THRESHOLD
        threshold = max(percent_threshold, self.FIXED_THRESHOLD)

        return candle_range >= threshold


    # --------------------------------------------------------
    # MAIN CALCULATION
    # --------------------------------------------------------

    def calculate(self):

        if not self.is_trading_day():
            return None

        df = self.candle_buffer

        if df is None or len(df) < 2:
            return None

        tcol = _resolve_time_column(df)
        if tcol is None:
            return None

        today = df.iloc[-1][tcol].date()

        if self.session_date == today and self.cached_orb is not None:
            return self.cached_orb

        first = self.get_first_candle()

        if first is None:
            return None

        self.prevDayClose = df.iloc[-2]["close"]

        open_price = first["open"]
        high = first["high"]
        low = first["low"]
        close = first["close"]

        if (high - low) > self.MAX_RANGE:
            self.rangeTooBig = True

        if self.rangeTooBig and len(df) >= 3:

            second = df.iloc[-1]

            open_price = second["open"]
            high = second["high"]
            low = second["low"]
            close = second["close"]

            self.rangeTooBig = False

        halfback = (high + low) / 2
        orbHigh = high
        orbLow = low

        if not self.is_long_candle(orbHigh, orbLow):

            result = {
                "type": "normal_opening_candle",
                "orbHigh": orbHigh,
                "orbLow": orbLow,
                "halfback": halfback,
                "prevDayClose": self.prevDayClose
            }

        elif close >= halfback:

            result = {
                "type": "bullish_long_opening",
                "orbHigh": orbHigh,
                "orbLow": halfback,
                "prevDayClose": self.prevDayClose
            }

        else:

            result = {
                "type": "bearish_long_opening",
                "orbHigh": halfback,
                "orbLow": orbLow,
                "prevDayClose": self.prevDayClose
            }

        self.session_date = today
        self.cached_orb = result

        return result


# ============================================================
# VWAP INDICATOR
# ============================================================

class VWAPIndicator:

    def __init__(self, timeframe, instrument, candle_buffer,
                 calc_mode="Standard Deviation"):

        self.timeframe = timeframe
        self.instrument = instrument
        self.candle_buffer = candle_buffer
        self.calc_mode = calc_mode

        self.band_mult_1 = 0.5
        self.band_mult_2 = 1.0
        self.band_mult_3 = 1.5


    def _validate(self):

        if self.candle_buffer is None:
            return False

        if len(self.candle_buffer) == 0:
            return False

        required = {"high", "low", "close", "volume"}

        return required.issubset(self.candle_buffer.columns)


    def get_today_candles(self):

        df = self.candle_buffer

        if not self._validate():
            return None

        tcol = _resolve_time_column(df)
        if tcol is None:
            return None

        today = df.iloc[-1][tcol].date()

        df = df[df[tcol].dt.date == today]

        if df.empty:
            return None

        return df


    def _compute_vwap_core(self):

        df = self.get_today_candles()

        if df is None:
            return None

        price = (df["high"] + df["low"] + df["close"]) / 3
        volume = df["volume"]

        pv = (price * volume).cumsum()
        cum_vol = volume.cumsum()

        vwap_series = pv / cum_vol
        vwap = float(vwap_series.iloc[-1])

        deviation = price - vwap_series
        variance = (volume * deviation ** 2).cumsum() / cum_vol
        std = float(np.sqrt(variance.iloc[-1]))

        basis = std if self.calc_mode == "Standard Deviation" else vwap * 0.01

        return {

            "vwap": vwap,

            "up1": vwap + basis * self.band_mult_1,
            "dn1": vwap - basis * self.band_mult_1,

            "up2": vwap + basis * self.band_mult_2,
            "dn2": vwap - basis * self.band_mult_2,

            "up3": vwap + basis * self.band_mult_3,
            "dn3": vwap - basis * self.band_mult_3
        }


    def calculate(self):

        return self._compute_vwap_core()


# ============================================================
# MARKET PROFILE INDICATOR
# ============================================================

class MarketProfileIndicator:

    IST = pytz.timezone("Asia/Kolkata")

    def __init__(self, timeframe, instrument, candle_buffer,
                 tpo_size=20, value_area_percent=70):

        self.timeframe = timeframe
        self.instrument = instrument
        self.candle_buffer = candle_buffer

        self.tpo_size = tpo_size
        self.value_area_percent = value_area_percent

        self.session_date = None
        self.cached_profile = None


    def get_session_candles(self):

        df = self.candle_buffer

        if df is None or len(df) == 0:
            return None

        tcol = _resolve_time_column(df)
        if tcol is None:
            return None

        today = df.iloc[-1][tcol].date()

        df = df[df[tcol].dt.date == today]

        if df.empty:
            return None

        return df


    def _compute_profile_core(self):

        df = self.get_session_candles()

        if df is None:
            return None

        session_high = df["high"].max()
        session_low = df["low"].min()

        session_range = session_high - session_low

        if session_range == 0:
            return None

        tpo_diff = session_range / self.tpo_size

        tpo_values = []
        tpo_counts = []

        for x in range(self.tpo_size + 1):

            level = session_low + x * tpo_diff

            visits = ((df["low"] <= level) & (df["high"] >= level)).sum()

            tpo_values.append(level)
            tpo_counts.append(visits)

        tpo_counts = np.array(tpo_counts)

        poc_index = np.argmax(tpo_counts)
        poc_value = tpo_values[poc_index]

        total_tpo = tpo_counts.sum()
        target = total_tpo * self.value_area_percent / 100

        value_area = tpo_counts[poc_index]

        va_high = poc_index
        va_low = poc_index

        while value_area < target:

            up = min(self.tpo_size, va_high + 1)
            dn = max(0, va_low - 1)

            if tpo_counts[up] >= tpo_counts[dn]:
                va_high = up
                value_area += tpo_counts[up]
            else:
                va_low = dn
                value_area += tpo_counts[dn]

        vah = session_low + va_high * tpo_diff
        val = session_low + va_low * tpo_diff

        return {

            "instrument": self.instrument,
            "timeframe": self.timeframe,

            "sessionHigh": session_high,
            "sessionLow": session_low,

            "poc": poc_value,
            "vah": vah,
            "val": val,

            "tpo_values": tpo_values,
            "tpo_counts": tpo_counts.tolist(),
            "tpo_diff": tpo_diff
        }


    def calculate(self):

        df = self.candle_buffer

        if df is None or len(df) == 0:
            return None

        tcol = _resolve_time_column(df)
        if tcol is None:
            return None

        today = df.iloc[-1][tcol].date()

        if self.session_date == today and self.cached_profile is not None:
            return self.cached_profile

        result = self._compute_profile_core()

        self.session_date = today
        self.cached_profile = result

        return result
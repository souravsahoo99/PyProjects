# ============================================================
# STRATEGY UTILS
# PineScript Style Helpers + Indicator Extensions
# ============================================================

import numpy as np

from indicator_utils import VWAPIndicator, MarketProfileIndicator


# ============================================================
# CROSSOVER
# ============================================================

def cross_over(series_a, series_b):

    if series_a[1] is None:
        return False

    if series_b[1] is None:
        return False

    if series_a[1] <= series_b[1] and series_a[0] > series_b[0]:
        return True

    return False


def cross_under(series_a, series_b):

    if series_a[1] is None:
        return False

    if series_b[1] is None:
        return False

    if series_a[1] >= series_b[1] and series_a[0] < series_b[0]:
        return True

    return False


def cross(series_a, series_b):

    if cross_over(series_a, series_b):
        return True

    if cross_under(series_a, series_b):
        return True

    return False


# ============================================================
# BREAKOUT / BREAKDOWN
# ============================================================

def breakout(price, level):

    if price is None:
        return False

    if level is None:
        return False

    return price > level


def breakdown(price, level):

    if price is None:
        return False

    if level is None:
        return False

    return price < level


# ============================================================
# CANDLE STRUCTURE
# ============================================================

def inside_bar(high, low):

    if high[1] is None:
        return False

    if low[1] is None:
        return False

    return high[0] <= high[1] and low[0] >= low[1]


def higher_high(high):

    if high[1] is None:
        return False

    return high[0] > high[1]


def lower_low(low):

    if low[1] is None:
        return False

    return low[0] < low[1]


# ============================================================
# TREND STATE
# ============================================================

def trend_state(close, lookback=5):

    values = []

    for i in range(lookback):

        v = close[i]

        if v is None:
            return None

        values.append(v)

    if values[0] > values[-1]:
        return "UPTREND"

    if values[0] < values[-1]:
        return "DOWNTREND"

    return "SIDEWAYS"


# ============================================================
# RISK MANAGEMENT
# ============================================================

def stop_distance(entry, stop):

    if entry is None or stop is None:
        return None

    return abs(entry - stop)


def risk_reward(entry, stop, target):

    if entry is None or stop is None or target is None:
        return None

    risk = abs(entry - stop)
    reward = abs(target - entry)

    if risk == 0:
        return None

    return reward / risk


def position_size(account_risk, stop_distance):

    if stop_distance is None or stop_distance == 0:
        return None

    return int(account_risk / stop_distance)


# ============================================================
# PIVOTS
# ============================================================

def pivot_high(high, left, right):

    center = right
    pivot = high[center]

    if pivot is None:
        return None

    for i in range(1, left + 1):

        if high[center + i] >= pivot:
            return None

    for i in range(1, right + 1):

        if high[center - i] >= pivot:
            return None

    return pivot


def pivot_low(low, left, right):

    center = right
    pivot = low[center]

    if pivot is None:
        return None

    for i in range(1, left + 1):

        if low[center + i] <= pivot:
            return None

    for i in range(1, right + 1):

        if low[center - i] <= pivot:
            return None

    return pivot


def swing_high(high, left, right):
    return pivot_high(high, left, right)


def swing_low(low, left, right):
    return pivot_low(low, left, right)


# ============================================================
# PINE STYLE HELPERS
# ============================================================

def highest(series, length):

    values = []

    for i in range(length):

        v = series[i]

        if v is None:
            return None

        values.append(v)

    return max(values)


def lowest(series, length):

    values = []

    for i in range(length):

        v = series[i]

        if v is None:
            return None

        values.append(v)

    return min(values)


def bars_since(condition_series, lookback=100):

    for i in range(lookback):

        if condition_series[i]:
            return i

    return None


def value_when(condition_series, value_series, occurrence=0, lookback=100):

    count = 0

    for i in range(lookback):

        if condition_series[i]:

            if count == occurrence:
                return value_series[i]

            count += 1

    return None


# ============================================================
# VWAP BAND STATE (Child of VWAPIndicator)
# ============================================================

class VWAPBandState(VWAPIndicator):

    def calculate_state(self):

        base = self._compute_vwap_core()

        if base is None:
            return None

        vwap = base["vwap"]
        up1 = base["up1"]
        dn1 = base["dn1"]

        latest_close = self.candle_buffer.iloc[-1]["close"]

        priceUp = latest_close > vwap
        priceDn = latest_close < vwap
        inRange = (latest_close < up1) and (latest_close > dn1)

        return {

            "instrument": self.instrument,
            "timeframe": self.timeframe,

            "vwap": vwap,

            "priceUp": priceUp,
            "priceDn": priceDn,
            "inRange": inRange
        }


# ============================================================
# VWAP MEMORY MODULE
# ============================================================

class VwapMemoryModule(VWAPIndicator):

    def __init__(self, timeframe, instrument, candle_buffer,
                 calc_mode="Standard Deviation", max_sessions=3):

        super().__init__(timeframe, instrument, candle_buffer, calc_mode)

        self.max_sessions = max_sessions

        self.vwapMemory = []
        self.upperBand1Memory = []
        self.lowerBand1Memory = []
        self.upperBand2Memory = []
        self.lowerBand2Memory = []
        self.vwapBandRangeMemory = []

        self.last_session_date = None


    def update_memory(self):

        result = self._compute_vwap_core()

        if result is None:
            return None

        latest = self.candle_buffer.iloc[-1]
        session_date = latest["datetime"].date()

        if self.last_session_date == session_date:
            return

        self.last_session_date = session_date

        vwap = result["vwap"]
        up1 = result["up1"]
        dn1 = result["dn1"]
        up2 = result["up2"]
        dn2 = result["dn2"]

        band_range = up1 - dn1

        self.vwapMemory.insert(0, vwap)
        self.upperBand1Memory.insert(0, up1)
        self.lowerBand1Memory.insert(0, dn1)
        self.upperBand2Memory.insert(0, up2)
        self.lowerBand2Memory.insert(0, dn2)
        self.vwapBandRangeMemory.insert(0, band_range)

        if len(self.vwapMemory) > self.max_sessions:

            self.vwapMemory.pop()
            self.upperBand1Memory.pop()
            self.lowerBand1Memory.pop()
            self.upperBand2Memory.pop()
            self.lowerBand2Memory.pop()
            self.vwapBandRangeMemory.pop()


# ============================================================
# TPO MEMORY MODULE
# ============================================================

class TpoMemoryModule(MarketProfileIndicator):

    def __init__(self, timeframe, instrument, candle_buffer,
                 tpo_size=20, value_area_percent=70, max_sessions=6):

        super().__init__(timeframe, instrument, candle_buffer,
                         tpo_size, value_area_percent)

        self.max_sessions = max_sessions

        self.prevVAH_array = []
        self.prevVAL_array = []
        self.prevPOC_array = []
        self.prevRange_array = []

        self.last_session_date = None


    def update_memory(self):

        result = self._compute_profile_core()

        if result is None:
            return None

        latest = self.candle_buffer.iloc[-1]
        session_date = latest["datetime"].date()

        if self.last_session_date == session_date:
            return

        self.last_session_date = session_date

        vah = result["vah"]
        val = result["val"]
        poc = result["poc"]

        tpo_range = vah - val

        self.prevVAH_array.insert(0, vah)
        self.prevVAL_array.insert(0, val)
        self.prevPOC_array.insert(0, poc)
        self.prevRange_array.insert(0, tpo_range)

        if len(self.prevVAH_array) > self.max_sessions:

            self.prevVAH_array.pop()
            self.prevVAL_array.pop()
            self.prevPOC_array.pop()
            self.prevRange_array.pop()


# ============================================================
# END
# ============================================================


def avg_body_size(buffer, length=10):

    if buffer is None or len(buffer) < length:
        return None

    bodies = []

    opens = list(buffer.open)
    closes = list(buffer.close)

    for i in range(1, length + 1):

        body = abs(closes[-i] - opens[-i])
        bodies.append(body)

    return sum(bodies) / len(bodies)


def body_strength(buffer, length=10):

    if buffer is None or len(buffer) < length + 1:
        return False

    avg_body = avg_body_size(buffer, length)

    if avg_body is None:
        return False

    current_body = abs(buffer.close[-1] - buffer.open[-1])

    return current_body > avg_body





#_#_
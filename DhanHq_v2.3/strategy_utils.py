# ============================================================
# STRATEGY UTILS
# ============================================================


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
# PINE SCRIPT STYLE HELPERS
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


#_
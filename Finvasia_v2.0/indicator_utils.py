# ============================================================
# INDICATOR UTILS
# ============================================================

import numpy as np
import pandas as pd
from datetime import datetime as dt


# ============================================================
# VWAP WITH STANDARD DEVIATIONS
# ============================================================

class VWAPBands:

    def __init__(self, df):

        self.df = df


    def calculate(self):

        price = (self.df["high"] + self.df["low"] + self.df["close"]) / 3

        volume = self.df["volume"]

        vwap = (price * volume).cumsum() / volume.cumsum()

        std = price.std()

        bands = {
            "vwap": vwap.iloc[-1],
            "upper1": vwap.iloc[-1] + std,
            "lower1": vwap.iloc[-1] - std,
            "upper2": vwap.iloc[-1] + (2 * std),
            "lower2": vwap.iloc[-1] - (2 * std),
            "upper3": vwap.iloc[-1] + (3 * std),
            "lower3": vwap.iloc[-1] - (3 * std),
        }

        return bands


# ============================================================
# OPENING RANGE BREAKOUT
# ============================================================

class OpeningRangeBreakout:

    def __init__(self, df, timeframe="1min"):

        self.df = df
        self.timeframe = timeframe

        self.range_high = None
        self.range_low = None


    def calculate(self):

        if len(self.df) == 0:

            return None

        first_candle = self.df.iloc[0]

        self.range_high = first_candle["high"]

        self.range_low = first_candle["low"]

        return {
            "high": self.range_high,
            "low": self.range_low
        }


# ============================================================
# MARKET PROFILE (TPO)
# ============================================================

class MarketProfile:

    def __init__(self, df):

        self.df = df


    def calculate(self):

        price_levels = np.round(self.df["close"], 1)

        counts = price_levels.value_counts()

        poc = counts.idxmax()

        value_area = counts.nlargest(int(len(counts) * 0.7)).index

        return {
            "poc": poc,
            "value_area": value_area.tolist()
        }


# ============================================================
# ANCHORED VOLUME PROFILE
# ============================================================

class AnchoredVolumeProfile:

    def __init__(self, df, anchor_index):

        self.df = df.iloc[anchor_index:]


    def calculate(self):

        volume_profile = self.df.groupby("close")["volume"].sum()

        poc = volume_profile.idxmax()

        return {
            "poc": poc,
            "profile": volume_profile
        }


# ============================================================
# SESSION VOLUME PROFILE
# ============================================================

class SessionVolumeProfile:

    def __init__(self, df):

        self.df = df


    def calculate(self):

        volume_profile = self.df.groupby("close")["volume"].sum()

        poc = volume_profile.idxmax()

        return {
            "poc": poc,
            "profile": volume_profile
        }
    

#_
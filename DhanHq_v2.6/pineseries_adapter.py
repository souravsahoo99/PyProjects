# ============================================================
# PINE SERIES ADAPTER
# PineScript-style series indexing
# ============================================================

class Series:

    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, index):

        if index < 0:
            return None

        if index >= len(self.data):
            return None

        try:
            return self.data[-(index + 1)]
        except Exception:
            return None


# ============================================================
# ADAPTER
# ============================================================

class SeriesAdapter:

    def __init__(self, series_dict):
        self.buffer = series_dict

    def open(self):

        data = self.buffer["open"]

        return Series(data)

    def high(self):

        data = self.buffer["high"]

        return Series(data)

    def low(self):

        data = self.buffer["low"]

        return Series(data)

    def close(self):

        data = self.buffer["close"]

        return Series(data)

    def volume(self):

        data = self.buffer["volume"]

        return Series(data)


#_#
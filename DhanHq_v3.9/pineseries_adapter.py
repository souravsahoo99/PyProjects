# ============================================================
# PINE SERIES ADAPTER v2.0
# PineScript-style series indexing
# Production Safe Version
# ============================================================


# ============================================================
# SERIES OBJECT
# ============================================================

class Series:

    def __init__(self, data):

        # create immutable snapshot to avoid concurrent mutation
        try:
            self.data = list(data)
        except Exception:
            self.data = []

        self.length = len(self.data)

    def __len__(self):

        return self.length


    # --------------------------------------------------------
    # Pine-style indexing
    # 0 = current candle
    # 1 = previous candle
    # --------------------------------------------------------

    def __getitem__(self, index):

        try:

            if index is None:
                return None

            if index < 0:
                return None

            if index >= self.length:
                return None

            return self.data[-(index + 1)]

        except Exception:
            return None


# ============================================================
# ADAPTER
# ============================================================

class SeriesAdapter:

    def __init__(self, series_dict):

        if series_dict is None:
            series_dict = {}

        self.buffer = series_dict


    # --------------------------------------------------------
    # SAFE ACCESSOR
    # --------------------------------------------------------

    def _get(self, key):

        data = self.buffer.get(key)

        if data is None:
            return Series([])

        return Series(data)


    # --------------------------------------------------------
    # SERIES ACCESS
    # --------------------------------------------------------

    def open(self):

        return self._get("open")


    def high(self):

        return self._get("high")


    def low(self):

        return self._get("low")


    def close(self):

        return self._get("close")


    def volume(self):

        return self._get("volume")
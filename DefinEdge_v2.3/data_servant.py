# ============================================================
# DATA SERVANT v2.0
# Unified Candle Data Gateway
# Threaded Runtime Version
# Shared Module
# ============================================================

import threading

from market_data import MarketDataManager


# ============================================================
# DATA SERVANT
# ============================================================

class DataServant:

    def __init__(self, engine):

        self.engine = engine

        # exchange|token → MarketDataManager
        self.pipeline_registry = {}

        # pipelines starting
        self._starting_pipelines = set()

        # thread safety
        self._lock = threading.Lock()


    # ========================================================
    # INTERNAL KEY
    # ========================================================

    def _key(self, exchange, token):

        return f"{exchange}|{token}"


    # ========================================================
    # SAFE PIPELINE START
    # ========================================================

    def _safe_start_pipeline(self, md, key):

        try:

            md.start()

        except Exception as e:

            print("[DATA SERVANT] Pipeline start failure:", key, e)

        finally:

            with self._lock:

                if key in self._starting_pipelines:

                    self._starting_pipelines.remove(key)


    # ========================================================
    # ENSURE PIPELINE
    # ========================================================

    def ensure_pipeline(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        with self._lock:

            md = self.pipeline_registry.get(key)

            # ------------------------------------------------
            # CHECK ENGINE REGISTRY
            # ------------------------------------------------

            if md is None:

                md = self.engine.market_data_map.get(key)

                if md:

                    self.pipeline_registry[key] = md


            # ------------------------------------------------
            # CREATE PIPELINE
            # ------------------------------------------------

            if md is None:

                md = self.engine.market_data_map.get(key)

                if md is None:

                    md = MarketDataManager(
                        self.engine,
                        exchange,
                        token,
                        required_timeframes=[timeframe]
                    )

                self.pipeline_registry[key] = md

                if key not in self._starting_pipelines:

                    self._starting_pipelines.add(key)

                    thread = threading.Thread(
                        target=self._safe_start_pipeline,
                        args=(md, key),
                        daemon=True
                    )

                    thread.start()

                return md


            # ------------------------------------------------
            # EXPAND TIMEFRAME
            # ------------------------------------------------

            if timeframe:

                md.ensure_timeframe(timeframe)

            return md


    # ========================================================
    # GET CANDLES
    # ========================================================

    def get_candles(self, exchange, token, timeframe):

        md = self.ensure_pipeline(exchange, token, timeframe)

        if md is None:
            return None

        return md.get(timeframe)


    # ========================================================
    # FAST CACHE ACCESS
    # ========================================================

    def get_cached(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        with self._lock:

            md = self.pipeline_registry.get(key)

            if md is None:
                return None

            return md.get(timeframe)
        

#_
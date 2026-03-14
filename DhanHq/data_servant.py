# ============================================================
# DATA SERVANT v1.0
# Unified Candle Data Gateway
# Shared Module
# ============================================================

import asyncio
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

        # async + thread safe access
        self._async_lock = asyncio.Lock()
        self._thread_lock = threading.Lock()


    # ========================================================
    # INTERNAL KEY
    # ========================================================

    def _key(self, exchange, token):

        return f"{exchange}|{token}"


    # ========================================================
    # ENSURE PIPELINE
    # ========================================================

    async def ensure_pipeline(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        async with self._async_lock:

            md = self.pipeline_registry.get(key)

            # ------------------------------------------------
            # CREATE PIPELINE
            # ------------------------------------------------

            if md is None:

                md = MarketDataManager(
                    self.engine,
                    exchange,
                    token,
                    required_timeframes=[timeframe]
                )

                self.pipeline_registry[key] = md

                asyncio.create_task(md.start())

                return md

            # ------------------------------------------------
            # EXPAND TIMEFRAME
            # ------------------------------------------------

            md.ensure_timeframe(timeframe)

            return md


    # ========================================================
    # GET CANDLES
    # ========================================================

    async def get_candles(self, exchange, token, timeframe):

        md = await self.ensure_pipeline(exchange, token, timeframe)

        return md.get(timeframe)


    # ========================================================
    # FAST CACHE ACCESS
    # ========================================================

    def get_cached(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        with self._thread_lock:

            md = self.pipeline_registry.get(key)

            if md is None:
                return None

            return md.get(timeframe)


# ============================================================
# END
# ============================================================
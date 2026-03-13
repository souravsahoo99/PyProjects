# ============================================================
# INSTRUMENT NODE
# Production Node Controller
# Compatible with DataServant Architecture
# ============================================================

import asyncio
import threading

from market_data import MarketDataManager
from signal_engine import SignalEngine


# ============================================================
# DATA SERVANT (Embedded)
# ============================================================

class DataServant:

    def __init__(self, engine):

        self.engine = engine

        # exchange|token → MarketDataManager
        self.pipeline_registry = {}

        self._lock = threading.Lock()


    # --------------------------------------------------------
    # INTERNAL KEY
    # --------------------------------------------------------

    def _key(self, exchange, token):

        return f"{exchange}|{token}"


    # --------------------------------------------------------
    # ENSURE PIPELINE
    # --------------------------------------------------------

    async def ensure_pipeline(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        with self._lock:

            md = self.pipeline_registry.get(key)

            # create pipeline if missing
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

            # expand timeframe if needed
            md.ensure_timeframe(timeframe)

            return md


    # --------------------------------------------------------
    # GET CANDLES
    # --------------------------------------------------------

    async def get_candles(self, exchange, token, timeframe):

        md = await self.ensure_pipeline(exchange, token, timeframe)

        return md.get(timeframe)


    # --------------------------------------------------------
    # FAST ACCESS (CACHE)
    # --------------------------------------------------------

    def get_cached(self, exchange, token, timeframe):

        key = self._key(exchange, token)

        md = self.pipeline_registry.get(key)

        if md is None:
            return None

        return md.get(timeframe)


# ============================================================
# INSTRUMENT NODE
# ============================================================

class InstrumentNode:

    def __init__(
        self,
        engine,
        exchange,
        symbol,
        token
    ):

        # ----------------------------------------------------
        # ENGINE / INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.engine = engine
        self.exchange = exchange
        self.symbol = symbol
        self.token = token

        # ----------------------------------------------------
        # MODULE INSTANCES
        # ----------------------------------------------------

        self.market_data = None
        self.signal_engine = None
        self.servant = None

        # ----------------------------------------------------
        # ASYNC TASKS
        # ----------------------------------------------------

        self.tasks = []


    # ========================================================
    # INITIALIZE NODE
    # ========================================================

    async def initialize(self):

        print(f"[NODE] Initializing → {self.symbol}")

        # ----------------------------------------------------
        # SUBSCRIBE WEBSOCKET
        # ----------------------------------------------------

        self.engine.subscribe(self.exchange, self.token)

        # ----------------------------------------------------
        # CREATE DATA SERVANT
        # ----------------------------------------------------

        self.servant = DataServant(self.engine)

        # ----------------------------------------------------
        # SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine = SignalEngine(
            engine=self.engine,
            market_data=None,
            symbol=self.symbol,
            token=self.token,
            publisher=None
        )

        required_timeframes = self.signal_engine.get_required_timeframes()

        if not required_timeframes:
            required_timeframes = ["1m"]

        print(f"[NODE] {self.symbol} required pipelines → {required_timeframes}")

        # ----------------------------------------------------
        # BASE MARKET DATA PIPELINE
        # ----------------------------------------------------

        self.market_data = MarketDataManager(
            self.engine,
            self.exchange,
            self.token,
            required_timeframes
        )

        await self.market_data.start()

        # ----------------------------------------------------
        # REGISTER BASE PIPELINE WITH SERVANT
        # ----------------------------------------------------

        key = f"{self.exchange}|{self.token}"

        self.servant.pipeline_registry[key] = self.market_data

        # ----------------------------------------------------
        # CONNECT TO SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine.market_data = self.market_data
        self.signal_engine.servant = self.servant


    # ========================================================
    # START NODE
    # ========================================================

    def start(self):

        print(f"[NODE] Starting signal loop → {self.symbol}")

        if self.signal_engine is None:
            return

        task = asyncio.create_task(
            self.signal_engine.run()
        )

        self.tasks.append(task)


    # ========================================================
    # RETURN TASKS
    # ========================================================

    def get_tasks(self):

        if self.tasks is None:
            return []

        return self.tasks




#_#_#_#_#
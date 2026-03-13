# ============================================================
# INSTRUMENT NODE
# Production Node Controller
# Compatible with DataServant Architecture
# ============================================================

import asyncio

from market_data import MarketDataManager
from signal_engine import SignalEngine


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

        # InstrumentNode only creates the BASE pipeline
        # Additional pipelines are handled dynamically
        # by the DataServant layer inside SignalEngine

        self.market_data = MarketDataManager(
            self.engine,
            self.exchange,
            self.token,
            required_timeframes
        )

        await self.market_data.start()

        # ----------------------------------------------------
        # CONNECT DATA PIPELINE TO SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine.market_data = self.market_data


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
# ============================================================
# INSTRUMENT NODE
# Production Node Controller
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
        # Subscribe instrument to WebSocket
        # ----------------------------------------------------

        self.engine.subscribe(self.exchange, self.token)

        # ----------------------------------------------------
        # MARKET DATA MANAGER
        # ----------------------------------------------------

        self.market_data = MarketDataManager(
            self.engine,
            self.exchange,
            self.token
        )

        await self.market_data.start()

        # ----------------------------------------------------
        # SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine = SignalEngine(
            self.market_data,
            self.symbol,
            self.token
        )


    # ========================================================
    # START NODE
    # ========================================================

    def start(self):

        print(f"[NODE] Starting signal loop → {self.symbol}")

        self.tasks.append(
            asyncio.create_task(
                self.signal_engine.run()
            )
        )


    # ========================================================
    # RETURN TASKS
    # ========================================================

    def get_tasks(self):

        return self.tasks


#_#_#_
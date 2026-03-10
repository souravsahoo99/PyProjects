# ============================================================
# INSTRUMENT NODE
# Production Signal Node
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
        signal_exchange,
        signal_symbol,
        signal_token
    ):

        # ----------------------------------------------------
        # ENGINE
        # ----------------------------------------------------

        self.engine = engine

        # ----------------------------------------------------
        # SIGNAL INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.signal_exchange = signal_exchange
        self.signal_symbol = signal_symbol
        self.signal_token = signal_token

        # ----------------------------------------------------
        # MODULE INSTANCES
        # ----------------------------------------------------

        self.market_data = None
        self.signal_engine = None

        # ----------------------------------------------------
        # TASK STORAGE
        # ----------------------------------------------------

        self.tasks = []


    # ========================================================
    # INITIALIZE NODE
    # ========================================================

    async def initialize(self):

        print(f"[NODE] Initializing signal node → {self.signal_symbol}")

        # ----------------------------------------------------
        # WEBSOCKET SUBSCRIPTION
        # ----------------------------------------------------

        self.engine.subscribe(self.signal_exchange,self.signal_token)

        # ----------------------------------------------------
        # MARKET DATA MANAGER
        # ----------------------------------------------------

        self.market_data = MarketDataManager(
            self.engine,
            self.signal_exchange,
            self.signal_token
        )
        

        await self.market_data.start()

        # ----------------------------------------------------
        # SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine = SignalEngine(

            self.market_data,
            self.signal_symbol,
            self.signal_token

        )


    # ========================================================
    # START NODE
    # ========================================================

    def start(self):

        print(f"[NODE] Starting signal loop → {self.signal_symbol}")

        signal_task = asyncio.create_task(
            self.signal_engine.run()
        )

        self.tasks.append(signal_task)


    # ========================================================
    # RETURN TASKS
    # ========================================================

    def get_tasks(self):

        return self.tasks




#_#_#_#
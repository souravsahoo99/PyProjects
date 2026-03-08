# ============================================================
# INSTRUMENT NODE
# Production Node Controller
# ============================================================

import asyncio

from market_data import MarketDataManager
from signal_engine import SignalEngine
from trade_manager import TradeManager


# ============================================================
# INSTRUMENT NODE
# ============================================================

class InstrumentNode:

    def __init__(
        self,
        engine,
        exchange,
        symbol,
        parent_token,
        child_token,
        qty
    ):

        # ----------------------------------------------------
        # ENGINE / INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.engine = engine
        self.exchange = exchange
        self.symbol = symbol

        # ----------------------------------------------------
        # TOKEN ROLES
        # ----------------------------------------------------

        # signal source
        self.parent_token = parent_token

        # execution instrument
        self.child_token = child_token

        self.qty = qty

        # ----------------------------------------------------
        # MODULE INSTANCES
        # ----------------------------------------------------

        self.market_data = None
        self.signal_engine = None
        self.trade_manager = None

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
        # Subscribe execution instrument to WebSocket
        # ----------------------------------------------------

        self.engine.subscribe(self.exchange, self.child_token)

        # ----------------------------------------------------
        # MARKET DATA MANAGER
        # ----------------------------------------------------

        self.market_data = MarketDataManager(
            self.engine,
            self.exchange,
            self.child_token
        )

        await self.market_data.start()

        # ----------------------------------------------------
        # SIGNAL ENGINE
        # ----------------------------------------------------

        self.signal_engine = SignalEngine(
            self.market_data,
            self.symbol,
            self.parent_token
        )

        # ----------------------------------------------------
        # TRADE MANAGER
        # ----------------------------------------------------

        self.trade_manager = TradeManager(
            self.engine,
            self.exchange,
            self.symbol,
            self.parent_token,
            self.child_token,
            self.qty,
            None,
            None
        )


    # ========================================================
    # START NODE
    # ========================================================

    def start(self):

        print(f"[NODE] Starting loops → {self.symbol}")

        # ----------------------------------------------------
        # SIGNAL ENGINE LOOP
        # ----------------------------------------------------

        self.tasks.append(
            asyncio.create_task(
                self.signal_engine.run()
            )
        )

        # ----------------------------------------------------
        # TRADE MANAGER LOOP
        # ----------------------------------------------------

        self.tasks.append(
            asyncio.create_task(
                self.trade_manager.run()
            )
        )


    # ========================================================
    # RETURN TASKS
    # ========================================================

    def get_tasks(self):

        return self.tasks



#_#
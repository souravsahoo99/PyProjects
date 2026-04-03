# ============================================================
# INSTRUMENT NODE v2.2
# Production Node Controller
# Compatible with Shared DataServant Architecture
# Strategy-Orchestration Compatible
# Resilience Guards Enabled (Checkpoint 7.0)
# ============================================================

import asyncio

from market_data import MarketDataManager
from signal_engine import SignalEngine
from data_servant import DataServant


# ============================================================
# INSTRUMENT NODE
# ============================================================

class InstrumentNode:

    def __init__(
        self,
        engine,
        exchange,
        symbol,
        token,
        instrument_type=None,
        node_scope="PARENT"
    ):

        # ----------------------------------------------------
        # ENGINE / INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.engine = engine
        self.exchange = exchange
        self.symbol = symbol
        self.token = token

        # ----------------------------------------------------
        # NODE INTELLIGENCE
        # ----------------------------------------------------

        self.instrument_type = instrument_type
        self.node_scope = node_scope

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

        # ----------------------------------------------------
        # LIFECYCLE CONTROL
        # ----------------------------------------------------

        self._initialized = False
        self._running = False


    # ========================================================
    # SAFE SIGNAL ENGINE START
    # ========================================================

    async def _safe_signal_loop(self):

        try:
            await self.signal_engine.run()

        except Exception as e:

            print(f"[NODE ERROR] SignalEngine crashed → {self.symbol} | {e}")


    # ========================================================
    # INITIALIZE NODE
    # ========================================================

    async def initialize(self):

        if self._initialized:
            return

        print(f"[NODE] Initializing → {self.symbol}")

        # ----------------------------------------------------
        # SUBSCRIBE WEBSOCKET
        # ----------------------------------------------------

        self.engine.subscribe(self.exchange, self.token)

        # ----------------------------------------------------
        # CREATE SHARED DATA SERVANT
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
            publisher=None,

            instrument_type=self.instrument_type,
            node_scope=self.node_scope
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
        # CONNECT DATA SOURCES
        # ----------------------------------------------------

        self.signal_engine.market_data = self.market_data
        self.signal_engine.servant = self.servant

        self._initialized = True


    # ========================================================
    # START NODE
    # ========================================================

    def start(self):

        if not self._initialized:
            print(f"[NODE] Cannot start uninitialized node → {self.symbol}")
            return

        if self._running:
            return

        print(f"[NODE] Starting signal loop → {self.symbol}")

        task = asyncio.create_task(
            self._safe_signal_loop()
        )

        self.tasks.append(task)

        self._running = True


    # ========================================================
    # RETURN TASKS
    # ========================================================

    def get_tasks(self):

        if self.tasks is None:
            return []

        return self.tasks


    # ========================================================
    # STOP NODE
    # ========================================================

    def stop(self):

        self._running = False

        if self.signal_engine:
            self.signal_engine.stop()

        print(f"[NODE] Stopped → {self.symbol}")


# ============================================================
# END
# ============================================================





#_#_#_#_#_#
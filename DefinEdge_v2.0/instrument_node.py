# ============================================================
# INSTRUMENT NODE v4.0
# Production Node Controller
# Thread Runtime Compatible
# ============================================================

import threading

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
        node_scope="PARENT",
        registry=None
    ):

        # ----------------------------------------------------
        # ENGINE / INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.engine = engine
        self.exchange = exchange
        self.symbol = symbol
        self.token = token

        self.registry = registry

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
        # LIFECYCLE CONTROL
        # ----------------------------------------------------

        self._initialized = False
        self._running = False


# ============================================================
# INITIALIZE NODE
# ============================================================

    def initialize(self):

        if self._initialized:
            return

        print(f"[NODE] Initializing → {self.symbol}")

        # ----------------------------------------------------
        # ENSURE WEBSOCKET READY
        # ----------------------------------------------------

        try:
            self.engine.wait_for_ws()
        except Exception:
            pass

        # ----------------------------------------------------
        # SUBSCRIBE WEBSOCKET
        # ----------------------------------------------------

        self.engine.subscribe(self.exchange, self.token)

        # ----------------------------------------------------
        # CREATE DATA SERVANT
        # ----------------------------------------------------

        self.servant = DataServant(self.engine)

        # ----------------------------------------------------
        # CREATE SIGNAL ENGINE
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

        print(
            f"[NODE] {self.symbol} required pipelines → {required_timeframes}"
        )

        # ----------------------------------------------------
        # CREATE MARKET DATA PIPELINE
        # ----------------------------------------------------

        self.market_data = MarketDataManager(

            self.engine,
            self.exchange,
            self.token,
            required_timeframes

        )

        self.market_data.start()

        # ----------------------------------------------------
        # REGISTER PIPELINE WITH SERVANT
        # ----------------------------------------------------

        key = f"{self.exchange}|{self.token}"

        self.servant.pipeline_registry[key] = self.market_data

        # ----------------------------------------------------
        # CONNECT DATA SOURCES
        # ----------------------------------------------------

        self.signal_engine.market_data = self.market_data

        self.signal_engine.servant = self.servant

        self._initialized = True


# ============================================================
# START NODE
# ============================================================

    def start(self):

        if not self._initialized:

            print(
                f"[NODE] Cannot start uninitialized node → {self.symbol}"
            )

            return

        if self._running:
            return

        print(f"[NODE] Starting engine → {self.symbol}")

        self._running = True

        # Start SignalEngine thread
        self.signal_engine.start()


# ============================================================
# STOP NODE
# ============================================================

    def stop(self):

        self._running = False

        if self.signal_engine:
            self.signal_engine.stop()

        if self.market_data:
            self.market_data.stop()

        print(f"[NODE] Stopped → {self.symbol}")


# ============================================================
# COMPATIBILITY METHOD
# ============================================================

    def get_tasks(self):

        # kept for backward compatibility with older async code

        return []




#_#_
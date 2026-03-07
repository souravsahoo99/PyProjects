# ============================================================
# MAIN TRADING ENGINE
# ============================================================

import asyncio
import signal

from helper_wraper import ShoonyaEngine
from market_data import MarketDataManager
from signal_engine import SignalEngine
from trade_manager import TradeManager


# ============================================================
# INSTRUMENT CONFIGURATION
# ============================================================

# Each instrument is an independent trading node
# (MarketData + SignalEngine + TradeManager)

INSTRUMENTS = [

    # NSE INDEX
    {
        "exchange": "NSE",
        "symbol": "NIFTY 50",
        "token": "26000",
        "qty": 1
    },

    # NSE STOCK
    {
        "exchange": "NSE",
        "symbol": "RELIANCE",
        "token": "2885",
        "qty": 10
    },

    # NFO OPTION
    {
        "exchange": "NFO",
        "symbol": "NIFTY24JUN22000CE",
        "token": "40123",
        "qty": 50
    },

    # NFO FUTURE
    {
        "exchange": "NFO",
        "symbol": "NIFTY24JUNFUT",
        "token": "35001",
        "qty": 50
    },

    # MCX COMMODITY
    {
        "exchange": "MCX",
        "symbol": "GOLDM24JUNFUT",
        "token": "50012",
        "qty": 1
    }

]


# ============================================================
# ENGINE BOOTLOADER
# ============================================================

async def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    # --------------------------------------------------------
    # 1. Initialize Broker Engine
    # --------------------------------------------------------

    engine = ShoonyaEngine()

    # container for async tasks
    tasks = []

    # --------------------------------------------------------
    # 2. Start WebSocket once (shared across instruments)
    # --------------------------------------------------------

    print("[ENGINE] Starting WebSocket")

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")


    # --------------------------------------------------------
    # 3. Initialize Each Instrument Node
    # --------------------------------------------------------

    for inst in INSTRUMENTS:

        exchange = inst["exchange"]
        symbol   = inst["symbol"]
        token    = inst["token"]
        qty      = inst["qty"]

        print(f"[ENGINE] Initializing instrument → {symbol}")


        # ----------------------------------------------------
        # Subscribe instrument to broker WebSocket
        # ----------------------------------------------------

        engine.subscribe(exchange, token)


        # ----------------------------------------------------
        # Create Market Data Manager
        # Handles tick aggregation and candle creation
        # ----------------------------------------------------

        market_data = MarketDataManager(
            engine,
            exchange,
            token
        )

        # start REST candle pipelines
        await market_data.start()


        # ----------------------------------------------------
        # Create Signal Engine
        # Responsible for strategy evaluation
        # ----------------------------------------------------

        signal_engine = SignalEngine(
            market_data,
            symbol,
            token
        )


        # ----------------------------------------------------
        # Create Trade Manager
        # Responsible for order execution and position state
        # ----------------------------------------------------

        trade_manager = TradeManager(
            engine,
            exchange,
            symbol,
            token,
            qty,
            None,
            None
        )


        # ----------------------------------------------------
        # Launch Signal Engine async loop
        # ----------------------------------------------------

        tasks.append(
            asyncio.create_task(
                signal_engine.run()
            )
        )


        # ----------------------------------------------------
        # Launch Trade Manager async loop
        # ----------------------------------------------------

        tasks.append(
            asyncio.create_task(
                trade_manager.run()
            )
        )


    print("\n[ENGINE] All instrument nodes initialized\n")


    # --------------------------------------------------------
    # 4. Run all async tasks together
    # --------------------------------------------------------

    await asyncio.gather(*tasks)


# ============================================================
# GRACEFUL SHUTDOWN HANDLER
# ============================================================

def shutdown():

    print("\n[ENGINE] Shutdown requested\n")


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        # create new event loop explicitly
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # register OS signal handlers for graceful exit
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)

        # start engine
        loop.run_until_complete(engine_bootloader())

    except KeyboardInterrupt:

        print("\n[ENGINE] Interrupted by user\n")

    finally:

        print("[ENGINE] Trading Engine stopped\n")
# ============================================================
# MAIN TRADING ENGINE
# ============================================================

import asyncio
import signal

from helper_wraper import ShoonyaEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode


# ============================================================
# TRADING CONFIGURATION
# ============================================================

TRADING_CONFIG = [

    # --------------------------------------------------------
    # EQUITY
    # --------------------------------------------------------

    {
        "type": "equity",
        "exchange": "NSE",
        "symbol": "RELIANCE",
        "qty": 10
    },

    # --------------------------------------------------------
    # OPTION STRATEGY
    # --------------------------------------------------------

    {
        "type": "options",
        "exchange": "NFO",
        "symbol": "NIFTY",
        "expiry": "2024-06-27",
        "window": 5,
        "qty": 50
    },

    # --------------------------------------------------------
    # FUTURE STRATEGY
    # --------------------------------------------------------

    {
        "type": "future",
        "exchange": "NFO",
        "symbol": "NIFTY",
        "qty": 50
    }

]


# ============================================================
# BUILD INSTRUMENT LIST
# ============================================================

def build_instruments(registry):

    instruments = []

    for config in TRADING_CONFIG:

        inst_type = config["type"]

        # ----------------------------------------------------
        # EQUITY
        # ----------------------------------------------------

        if inst_type == "equity":

            token = registry.get_token(
                config["exchange"],
                config["symbol"]
            )

            if token:

                instruments.append({
                    "exchange": config["exchange"],
                    "symbol": config["symbol"],
                    "token": token,
                    "qty": config["qty"]
                })

        # ----------------------------------------------------
        # FUTURE
        # ----------------------------------------------------

        elif inst_type == "future":

            fut = registry.get_current_future(
                config["symbol"]
            )

            if fut:

                instruments.append({
                    "exchange": fut.exchange,
                    "symbol": fut.symbol,
                    "token": fut.token,
                    "qty": config["qty"]
                })

        # ----------------------------------------------------
        # OPTIONS
        # ----------------------------------------------------

        elif inst_type == "options":

            # placeholder spot price
            # (later this can come from index tick)
            spot = 20000

            contracts = registry.build_option_universe(
                symbol=config["symbol"],
                expiry=config["expiry"],
                spot=spot,
                window=config["window"]
            )

            for c in contracts:

                instruments.append({
                    "exchange": c.exchange,
                    "symbol": c.symbol,
                    "token": c.token,
                    "qty": config["qty"]
                })

    return instruments


# ============================================================
# ENGINE BOOTLOADER
# ============================================================

async def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    # --------------------------------------------------------
    # 1. Initialize Broker Engine
    # --------------------------------------------------------

    engine = ShoonyaEngine()

    # --------------------------------------------------------
    # 2. Load Token Registry
    # --------------------------------------------------------

    print("[ENGINE] Loading instrument registry")

    registry = TokenRegistry()

    registry.load_master("data/instruments.csv")

    print("[ENGINE] Registry loaded")

    # --------------------------------------------------------
    # 3. Start WebSocket
    # --------------------------------------------------------

    print("[ENGINE] Starting WebSocket")

    engine.start_ws()

    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    # --------------------------------------------------------
    # 4. Build Instrument Universe
    # --------------------------------------------------------

    runtime_instruments = build_instruments(registry)

    print(f"[ENGINE] Instruments discovered → {len(runtime_instruments)}")

    # --------------------------------------------------------
    # 5. Create Instrument Nodes
    # --------------------------------------------------------

    nodes = []

    for inst in runtime_instruments:

        node = InstrumentNode(
            engine,
            inst["exchange"],
            inst["symbol"],
            inst["token"],
            inst["qty"]
        )

        await node.initialize()

        node.start()

        nodes.append(node)

    print("\n[ENGINE] All nodes initialized\n")

    # --------------------------------------------------------
    # 6. Collect Async Tasks
    # --------------------------------------------------------

    tasks = []

    for node in nodes:

        tasks.extend(node.get_tasks())

    await asyncio.gather(*tasks)


# ============================================================
# SHUTDOWN HANDLER
# ============================================================

def shutdown():

    print("\n[ENGINE] Shutdown requested\n")


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)

        for sig in (signal.SIGINT, signal.SIGTERM):

            loop.add_signal_handler(sig, shutdown)

        loop.run_until_complete(engine_bootloader())

    except KeyboardInterrupt:

        print("\n[ENGINE] Interrupted by user\n")

    finally:

        print("[ENGINE] Trading Engine stopped\n")

#_#_#
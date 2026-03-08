# ============================================================
# MAIN TRADING ENGINE
# Checkpoint 4.0 – Production Orchestrator
# ============================================================

import asyncio
import signal
import time

from helper_wraper import ShoonyaEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode


# ============================================================
# TRADING CONFIGURATION
# ============================================================

TRADING_CONFIG = [

    {
        "type": "equity",
        "exchange": "NSE",
        "symbol": "RELIANCE",
        "qty": 10
    },

    {
        "type": "options",
        "exchange": "NFO",
        "symbol": "NIFTY",
        "expiry": "2024-06-27",
        "window": 5,
        "qty": 50
    },

    {
        "type": "future",
        "exchange": "NFO",
        "symbol": "NIFTY",
        "qty": 50
    }

]


# ============================================================
# BUILD RUNTIME INSTRUMENT LIST
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
                    "parent_token": token,
                    "child_token": token,
                    "qty": config["qty"]
                })


        # ----------------------------------------------------
        # FUTURE
        # ----------------------------------------------------

        elif inst_type == "future":

            fut = registry.get_current_future(config["symbol"])

            if fut:

                instruments.append({
                    "exchange": fut.exchange,
                    "symbol": fut.symbol,
                    "parent_token": fut.token,
                    "child_token": fut.token,
                    "qty": config["qty"]
                })


        # ----------------------------------------------------
        # OPTIONS
        # ----------------------------------------------------

        elif inst_type == "options":

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
                    "parent_token": c.token,
                    "child_token": c.token,
                    "qty": config["qty"]
                })

    return instruments


# ============================================================
# ENGINE BOOTLOADER
# ============================================================

async def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    # --------------------------------------------------------
    # Broker Engine
    # --------------------------------------------------------

    engine = ShoonyaEngine()

    # --------------------------------------------------------
    # Load Instrument Registry
    # --------------------------------------------------------

    print("[ENGINE] Loading instrument registry")

    registry = TokenRegistry()
    registry.load_master("data/instruments.csv")

    print("[ENGINE] Registry loaded")

    # --------------------------------------------------------
    # Start WebSocket
    # --------------------------------------------------------

    print("[ENGINE] Starting WebSocket")

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    # --------------------------------------------------------
    # Discover Instruments
    # --------------------------------------------------------

    runtime_instruments = build_instruments(registry)

    print(f"[ENGINE] Instruments discovered → {len(runtime_instruments)}")

    # --------------------------------------------------------
    # Create Instrument Nodes
    # --------------------------------------------------------

    nodes = []

    for inst in runtime_instruments:

        node = InstrumentNode(
            engine,
            inst["exchange"],
            inst["symbol"],
            inst["parent_token"],
            inst["child_token"],
            inst["qty"]
        )

        await node.initialize()

        node.start()

        nodes.append(node)

    print("\n[ENGINE] All nodes initialized\n")

    # --------------------------------------------------------
    # Collect async tasks
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

    restart_limit = 5
    restart_delay = 5
    restart_count = 0

    running = True

    while running:

        print(f"\n[SUPERVISOR] Engine start attempt {restart_count + 1}\n")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)

        try:

            loop.run_until_complete(engine_bootloader())

            running = False

        except KeyboardInterrupt:

            print("\n[ENGINE] Interrupted by user")
            running = False

        except Exception as e:

            restart_count += 1

            print(f"\n[ENGINE] Crash detected → {e}")
            print(f"[SUPERVISOR] Restart {restart_count}/{restart_limit}")

            if restart_count >= restart_limit:

                print("[SUPERVISOR] Restart limit reached. Stopping engine.")
                running = False

            else:

                print(f"[SUPERVISOR] Restarting in {restart_delay} seconds...\n")
                time.sleep(restart_delay)

        finally:

            loop.stop()
            loop.close()

    print("\n[ENGINE] Trading Engine stopped\n")


#_#_#_#_
# ============================================================
# MAIN TRADING ENGINE
# Production Orchestrator – Signal Layer + Execution Pool
# ============================================================

import asyncio
import signal
import time

from helper_wraper import ShoonyaEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode
from trade_manager import TradeManager


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
# BUILD SIGNAL NODES
# ============================================================

def build_signal_nodes(registry):

    nodes = []

    for config in TRADING_CONFIG:

        inst_type = config["type"]

        if inst_type == "equity":

            token = registry.get_token(
                config["exchange"],
                config["symbol"]
            )

            if token:

                nodes.append({
                    "exchange": config["exchange"],
                    "symbol": config["symbol"],
                    "token": token
                })


        elif inst_type == "future":

            fut = registry.get_current_future(config["symbol"])

            if fut:

                nodes.append({
                    "exchange": fut.exchange,
                    "symbol": fut.symbol,
                    "token": fut.token
                })


        elif inst_type == "options":

            spot = 20000

            contracts = registry.build_option_universe(
                symbol=config["symbol"],
                expiry=config["expiry"],
                spot=spot,
                window=config["window"]
            )

            for c in contracts:

                nodes.append({
                    "exchange": c.exchange,
                    "symbol": c.symbol,
                    "token": c.token
                })

    return nodes


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
    # Token Registry
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
    # Build Signal Nodes
    # --------------------------------------------------------

    node_configs = build_signal_nodes(registry)

    print(f"[ENGINE] Signal nodes discovered → {len(node_configs)}")

    nodes = []

    for inst in node_configs:

        node = InstrumentNode(
            engine,
            inst["exchange"],
            inst["symbol"],
            inst["token"]
        )

        await node.initialize()
        node.start()

        nodes.append(node)

    print("[ENGINE] Signal layer initialized\n")

    # --------------------------------------------------------
    # CREATE TRADE MANAGER POOL
    # --------------------------------------------------------

    trade_managers = []

    # Example execution strategy
    # parent = index signal
    # child = ATM option

    nifty_token = registry.get_token("NSE", "NIFTY")

    if nifty_token:

        fut = registry.get_current_future("NIFTY")

        if fut:

            tm = TradeManager(
                engine,
                fut.exchange,
                fut.symbol,
                parent_token=nifty_token,
                child_token=fut.token,
                qty=50,
                ws_ltp=None,
                rest_ltp=None
            )

            trade_managers.append(tm)

    print(f"[ENGINE] Trade managers created → {len(trade_managers)}\n")

    # --------------------------------------------------------
    # COLLECT ASYNC TASKS
    # --------------------------------------------------------

    tasks = []

    for node in nodes:
        tasks.extend(node.get_tasks())

    for tm in trade_managers:
        tasks.append(asyncio.create_task(tm.run()))

    # --------------------------------------------------------
    # RUN ENGINE
    # --------------------------------------------------------

    await asyncio.gather(*tasks)


# ============================================================
# SHUTDOWN HANDLER
# ============================================================

def shutdown():
    print("\n[ENGINE] Shutdown requested\n")


# ============================================================
# PROGRAM ENTRY
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

                print("[SUPERVISOR] Restart limit reached.")
                running = False

            else:

                print(f"[SUPERVISOR] Restarting in {restart_delay} seconds\n")
                time.sleep(restart_delay)

        finally:

            loop.stop()
            loop.close()

    print("\n[ENGINE] Trading Engine stopped\n")



#_#_#_#_
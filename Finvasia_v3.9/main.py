# ============================================================
# MAIN TRADING ENGINE
# Production Orchestrator (Checkpoint 6.3)
# ============================================================

import asyncio
import signal
import time

from helper_wraper import ShoonyaEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from signal_engine import SignalPublisher


# ============================================================
# STRATEGY CONFIGURATION
# ============================================================

STRATEGY_CONFIG = [

    {
        "parent_symbol": "NIFTY",
        "parent_exchange": "NSE",
        "child_exchange": "NFO",
        "product_type": "OPT",
        "qty": 65,
        "max_retry": 3
    },

    {
        "parent_symbol": "RELIANCE",
        "parent_exchange": "NSE",
        "child_exchange": "NSE",
        "product_type": "STOCK",
        "qty": 10,
        "max_retry": 2
    }

]


# ============================================================
# SAFE SPOT FETCH
# ============================================================

def wait_for_spot_price(engine, exchange, token, timeout=10):

    start = time.time()

    while True:

        price = engine.get_ltp_live(exchange, token)

        if price is not None:
            return price

        if time.time() - start > timeout:
            return None

        time.sleep(0.2)


# ============================================================
# ATM OPTION DISCOVERY
# ============================================================

def discover_atm_option_pair(engine, registry, symbol, exchange):

    parent_token = registry.get_token(exchange, symbol)

    if parent_token is None:
        return None, None

    spot = wait_for_spot_price(engine, exchange, parent_token)

    if spot is None:
        return None, None

    futures = registry.get_futures(symbol)

    expiry = None

    if futures:
        expiry = futures[0].expiry

    if expiry is None:
        return None, None

    strikes = registry.get_strikes(symbol, expiry)

    if not strikes:
        return None, None

    atm = min(strikes, key=lambda x: abs(x - spot))

    ce_token = registry.get_option_token(symbol, expiry, atm, "CE")
    pe_token = registry.get_option_token(symbol, expiry, atm, "PE")

    return ce_token, pe_token


# ============================================================
# BUILD SIGNAL NODES
# ============================================================

def build_signal_nodes(engine, registry):

    node_configs = []

    for config in STRATEGY_CONFIG:

        parent_symbol = config["parent_symbol"]
        parent_exchange = config["parent_exchange"]

        parent_token = registry.get_token(parent_exchange, parent_symbol)

        if parent_token is None:
            continue

        node_configs.append({

            "exchange": parent_exchange,
            "symbol": parent_symbol,
            "token": parent_token

        })

    return node_configs


# ============================================================
# BUILD TRADE MANAGERS
# ============================================================

def build_trade_managers(engine, registry):

    trade_managers = []

    for config in STRATEGY_CONFIG:

        parent_symbol = config["parent_symbol"]
        parent_exchange = config["parent_exchange"]
        child_exchange = config["child_exchange"]

        product_type = config["product_type"]
        qty = config["qty"]
        max_retry = config["max_retry"]

        parent_token = registry.get_token(parent_exchange, parent_symbol)

        if parent_token is None:
            continue

        child_token = parent_token
        trading_symbol = parent_symbol

        ce_token = None
        pe_token = None

        # ----------------------------------------------------
        # PRODUCT TYPE ROUTING
        # ----------------------------------------------------

        if product_type == "FUT":

            fut = registry.get_current_future(parent_symbol)

            if fut:
                child_token = fut.token
                trading_symbol = fut.symbol

        elif product_type == "OPT":

            ce_token, pe_token = discover_atm_option_pair(

                engine,
                registry,
                parent_symbol,
                parent_exchange

            )

            if ce_token is None or pe_token is None:
                continue

            child_token = ce_token

        elif product_type in ["SPOT", "STOCK"]:

            child_token = parent_token

        # ----------------------------------------------------
        # CREATE TRADE MANAGER
        # ----------------------------------------------------

        tm = TradeManager(

            engine=engine,
            parent_exchange=parent_exchange,
            child_exchange=child_exchange,

            signal_symbol=parent_symbol,
            trading_symbol=trading_symbol,

            parent_token=parent_token,
            child_token=child_token,

            product_type=product_type,

            qty=qty,

            ws_ltp=None,
            rest_ltp=None,

            max_retry=max_retry

        )

        # ----------------------------------------------------
        # OPTION TOKEN INJECTION
        # ----------------------------------------------------

        if product_type == "OPT":

            tm.ce_token = ce_token
            tm.pe_token = pe_token

        trade_managers.append(tm)

    return trade_managers


# ============================================================
# ENGINE BOOTLOADER
# ============================================================

async def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    engine = ShoonyaEngine()

    print("[ENGINE] Loading instrument registry")

    registry = TokenRegistry()

    registry.load_master("data/instruments.csv")

    print("[ENGINE] Registry loaded")

    # --------------------------------------------------------
    # START WEBSOCKET
    # --------------------------------------------------------

    print("[ENGINE] Starting WebSocket")

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    # --------------------------------------------------------
    # BUILD SIGNAL NODES
    # --------------------------------------------------------

    node_configs = build_signal_nodes(engine, registry)

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
    # BUILD TRADE MANAGERS
    # --------------------------------------------------------

    trade_managers = build_trade_managers(engine, registry)

    print(f"[ENGINE] Trade managers created → {len(trade_managers)}\n")

    # --------------------------------------------------------
    # SIGNAL PUBLISHER INJECTION
    # --------------------------------------------------------

    for tm in trade_managers:

        publisher = SignalPublisher(

            parent_token=tm.parent_token,
            child_token=tm.child_token,

            ce_token=tm.ce_token,
            pe_token=tm.pe_token,

            product_type=tm.product_type

        )

        for node in nodes:

            if node.token == tm.parent_token:

                node.signal_engine.publisher = publisher

    # --------------------------------------------------------
    # TASK COLLECTION
    # --------------------------------------------------------

    tasks = []

    for node in nodes:

        tasks.extend(node.get_tasks())

    for tm in trade_managers:

        tasks.append(asyncio.create_task(tm.run()))

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

    restart_limit = 2
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






#_#_#_#_#_#_#_#_
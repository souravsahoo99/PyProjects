# ============================================================
# MAIN TRADING ENGINE v5.0
# Global Token Bus Integrated | WS Lifecycle Correct
# ============================================================

import time
import threading

from helper_wraper import APIEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from display_monitor import DisplayMonitor
from global_token_bus import globalTokenMap


# ============================================================
# STRATEGY CONFIG
# ============================================================

STRATEGY_CONFIG = [

    {
        "parent_symbol": "NIFTY",
        "parent_exchange": "NSE",
        "parent_type": "FUT",
        "child_exchange": "NFO",
        "product_type": "OPT",
        "qty": 65,
        "max_retry": 3,
        "strike_dist": 50
    },

    {
        "parent_symbol": "RELIANCE",
        "parent_exchange": "NSE",
        "parent_type": "IDX",
        "child_exchange": "NSE",
        "product_type": "STOCK",
        "qty": 10,
        "max_retry": 2,
        "strike_dist": None
    }

]


# ============================================================
# SAFE SPOT FETCH
# ============================================================

def wait_for_spot_price(engine, exchange, token, timeout=10):

    start = time.time()

    while True:

        price = engine.get_ltp_rest(exchange, token)

        if price is not None:
            return price

        if time.time() - start > timeout:
            return None

        time.sleep(0.2)


# ============================================================
# STRIKE NORMALIZATION
# ============================================================

def normalize_strike(spot, strike_dist):

    if spot is None or not strike_dist:
        return None

    try:
        return int(round(float(spot) / strike_dist) * strike_dist)
    except Exception:
        return None


# ============================================================
# TOKEN RESOLUTION (UNCHANGED CORE)
# ============================================================

def resolve_parent_token(registry, exchange, symbol, parent_type):

    if parent_type == "FUT":

        futures = registry.get_futures(symbol)

        if futures:
            return futures[0].token

        return None

    return registry.get_token(exchange, symbol)


# ============================================================
# NODE SCOPE
# ============================================================

def resolve_node_scope(product_type):

    return "CHILD" if product_type == "OPT" else "PARENT"


# ============================================================
# BUILD SIGNAL NODES
# ============================================================

def build_signal_nodes(engine, registry):

    nodes = []

    for config in STRATEGY_CONFIG:

        symbol = config["parent_symbol"]
        exchange = config["parent_exchange"]
        parent_type = config.get("parent_type", "IDX")
        product_type = config.get("product_type", "STOCK")

        token = resolve_parent_token(registry, exchange, symbol, parent_type)

        if token is None:
            print(f"[ENGINE] Token resolution failed → {symbol}")
            continue

        node = InstrumentNode(
            engine,
            exchange,
            symbol,
            token,
            instrument_type=product_type,
            node_scope=resolve_node_scope(product_type)
        )

        nodes.append(node)

    return nodes


# ============================================================
# BUILD TRADE MANAGERS
# ============================================================

def build_trade_managers(engine, registry):

    managers = []

    for config in STRATEGY_CONFIG:

        parent_symbol = config["parent_symbol"]
        parent_exchange = config["parent_exchange"]
        parent_type = config.get("parent_type", "IDX")

        child_exchange = config["child_exchange"]
        product_type = config["product_type"]

        qty = config["qty"]
        max_retry = config["max_retry"]

        parent_token = resolve_parent_token(
            registry,
            parent_exchange,
            parent_symbol,
            parent_type
        )

        if parent_token is None:
            continue

        tm = TradeManager(
            engine=engine,
            parent_exchange=parent_exchange,
            child_exchange=child_exchange,
            signal_symbol=parent_symbol,
            trading_symbol=parent_symbol,
            parent_token=parent_token,
            child_token=parent_token,
            product_type=product_type,
            qty=qty,
            ws_ltp=None,
            rest_ltp=None,
            max_retry=max_retry
        )

        managers.append(tm)

    return managers


# ============================================================
# ENGINE BOOT
# ============================================================

def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    # --------------------------------------------------------
    # INIT ENGINE
    # --------------------------------------------------------

    engine = APIEngine()
    engine.market_data_map = {}

    # --------------------------------------------------------
    # INIT TOKEN REGISTRY (FIXED)
    # --------------------------------------------------------

    registry = TokenRegistry(api=engine.api)
    registry.load_master()

    # --------------------------------------------------------
    # REGISTER INSTRUMENTS (GLOBAL BUS)
    # --------------------------------------------------------

    for config in STRATEGY_CONFIG:

        symbol = config["parent_symbol"]
        exchange = config["parent_exchange"]
        symbol_type = config.get("parent_type", "IDX")

        try:
            registry.register_instrument(exchange, symbol, symbol_type)
        except Exception as e:
            print(f"[ENGINE] Registration failed → {symbol} | {e}")

    # --------------------------------------------------------
    # SUBSCRIBE USING GLOBAL TOKEN BUS
    # --------------------------------------------------------

    for inst in globalTokenMap:
        engine.subscribe(inst["exchange"], inst["token"])

    # --------------------------------------------------------
    # START WEBSOCKET (CORRECT ORDER)
    # --------------------------------------------------------

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    # --------------------------------------------------------
    # BUILD SYSTEM
    # --------------------------------------------------------

    nodes = build_signal_nodes(engine, registry)

    for node in nodes:
        node.initialize()
        node.start()

    trade_managers = build_trade_managers(engine, registry)

    monitor = DisplayMonitor(engine, trade_managers, nodes)

    threading.Thread(target=monitor.start, daemon=True).start()

    for tm in trade_managers:
        threading.Thread(target=tm.run, daemon=True).start()

    # --------------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------------

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[ENGINE] Interrupted by user")

    finally:

        print("[ENGINE] Shutting down")

        for node in nodes:
            node.stop()

        engine.shutdown()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    restart_limit = 3
    restart_delay = 5
    restart_count = 0

    while True:

        print(f"\n[SUPERVISOR] Engine start attempt {restart_count + 1}\n")

        try:
            engine_bootloader()
            break

        except KeyboardInterrupt:
            print("\n[ENGINE] Interrupted by user")
            break

        except Exception as e:

            restart_count += 1
            print(f"\n[ENGINE] Crash detected → {e}")

            if restart_count >= restart_limit:
                print("[SUPERVISOR] Restart limit reached.")
                break

            print(f"[SUPERVISOR] Restarting in {restart_delay} seconds\n")
            time.sleep(restart_delay)

    print("\n[ENGINE] Trading Engine stopped\n")




#_#_#_
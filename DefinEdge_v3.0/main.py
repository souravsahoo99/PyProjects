# ============================================================
# MAIN TRADING ENGINE v6.0
# ATM Options Integrated | Global Bus | WS Lifecycle Correct
# ============================================================

import time
import threading

from helper_wraper import APIEngine
from token_registry import TokenRegistry , TOKEN_BUS
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from display_monitor import DisplayMonitor

# ============================================================
#  STRATEGY CONFIG
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
# TOKEN RESOLUTION (UNCHANGED)
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
# BUILD TRADE MANAGERS (UPDATED FOR OPTIONS)
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
        strike_dist = config.get("strike_dist")

        parent_token = resolve_parent_token(
            registry,
            parent_exchange,
            parent_symbol,
            parent_type
        )

        if parent_token is None:
            continue

        child_token = parent_token
        ce_token = None
        pe_token = None

        # ----------------------------------------------------
        #  OPTION FLOW (NEW)
        # ----------------------------------------------------

        if product_type == "OPT":

            try:

                ce_token, pe_token = registry.register_atm_options(
                    engine,
                    parent_symbol,
                    parent_exchange,
                    strike_dist
                )

                child_token = ce_token

            except Exception as e:

                print(f"[ENGINE] ATM option registration failed → {parent_symbol} | {e}")
                continue

        tm = TradeManager(

            engine=engine,

            parent_exchange=parent_exchange,
            child_exchange=child_exchange,

            signal_symbol=parent_symbol,
            trading_symbol=parent_symbol,

            parent_token=parent_token,
            child_token=child_token,

            product_type=product_type,
            qty=qty,

            ws_ltp=None,
            rest_ltp=None,

            max_retry=max_retry
        )

        if product_type == "OPT":

            tm.ce_token = ce_token
            tm.pe_token = pe_token

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
    # INIT TOKEN REGISTRY
    # --------------------------------------------------------

    registry = TokenRegistry(api=engine.api)
    registry.load_master()

    # --------------------------------------------------------
    # REGISTER PARENT INSTRUMENTS
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
    # 🔥 REGISTER OPTIONS (BEFORE WS START)
    # --------------------------------------------------------

    for config in STRATEGY_CONFIG:

        if config["product_type"] == "OPT":

            try:
                registry.register_atm_options(
                    engine,
                    config["parent_symbol"],
                    config["parent_exchange"],
                    config["strike_dist"]
                )
            except Exception as e:
                print(f"[ENGINE] Pre-WS option registration failed → {config['parent_symbol']} | {e}")

    # --------------------------------------------------------
    # SUBSCRIBE USING GLOBAL TOKEN BUS
    # --------------------------------------------------------

    for inst in TOKEN_BUS:
        engine.subscribe(inst["exchange"], inst["token"])

    # --------------------------------------------------------
    # START WEBSOCKET
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





#_#_
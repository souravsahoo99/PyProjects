# ============================================================
# MAIN TRADING ENGINE v4.0
# Production Orchestrator
# Thread-Based Runtime
# ============================================================

import signal
import time
import sys
import threading

from helper_wraper import APIEngine
from token_registry import TokenRegistry
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from signal_engine import SignalPublisher

from candle_chart import CandleChart
from display_monitor import DisplayMonitor


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
        "max_retry": 3
    },

    {
        "parent_symbol": "RELIANCE",
        "parent_exchange": "NSE",
        "parent_type": "IDX",
        "child_exchange": "NSE",
        "product_type": "STOCK",
        "qty": 10,
        "max_retry": 2
    }

]

DEBUG_CHART_SYMBOL = None


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
# ATM OPTION DISCOVERY
# ============================================================

def discover_atm_option_pair(engine, registry, symbol, exchange):

    underlying_token = registry.get_token(exchange, symbol)

    if underlying_token is None:
        return None, None

    spot = wait_for_spot_price(engine, exchange, underlying_token)

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
# TOKEN RESOLUTION
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

    if product_type == "OPT":
        return "CHILD"

    return "PARENT"


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

        node_scope = resolve_node_scope(product_type)

        token = resolve_parent_token(
            registry,
            exchange,
            symbol,
            parent_type
        )

        if token is None:
            print(f"[ENGINE] Token resolution failed → {symbol}")
            continue

        node = InstrumentNode(
            engine,
            exchange,
            symbol,
            token,
            instrument_type=product_type,
            node_scope=node_scope
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

        child_token = parent_token
        trading_symbol = parent_symbol

        ce_token = None
        pe_token = None

        if product_type == "OPT":

            ce_token, pe_token = discover_atm_option_pair(
                engine,
                registry,
                parent_symbol,
                parent_exchange
            )

            if ce_token is None:
                continue

            child_token = ce_token

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

    engine = APIEngine()
    engine.market_data_map = {}

    registry = TokenRegistry()
    registry.load_master("data/instruments.csv")

    if not getattr(registry, "instruments", None):
        raise RuntimeError("Instrument registry not loaded")

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    nodes = build_signal_nodes(engine, registry)

    if not nodes:
        raise RuntimeError("No instrument nodes created")

    for node in nodes:

        node.initialize()
        node.start()

    trade_managers = build_trade_managers(engine, registry)

    monitor = DisplayMonitor(engine, trade_managers, nodes)

    monitor_thread = threading.Thread(
        target=monitor.start,
        daemon=True
    )

    monitor_thread.start()

    for tm in trade_managers:

        t = threading.Thread(
            target=tm.run,
            daemon=True
        )

        t.start()

    try:

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        print("\n[ENGINE] Interrupted by user")

    finally:

        print("[ENGINE] Stopping nodes")

        for node in nodes:
            node.stop()

        engine.shutdown()


# ============================================================
# PROGRAM ENTRY
# ============================================================

if __name__ == "__main__":

    restart_limit = 3
    restart_delay = 5

    restart_count = 0

    running = True

    while running:

        print(f"\n[SUPERVISOR] Engine start attempt {restart_count + 1}\n")

        try:

            engine_bootloader()

            running = False

        except KeyboardInterrupt:

            print("\n[ENGINE] Interrupted by user")
            running = False

        except Exception as e:

            restart_count += 1

            print(f"\n[ENGINE] Crash detected → {e}")

            if restart_count >= restart_limit:

                print("[SUPERVISOR] Restart limit reached.")
                running = False

            else:

                print(f"[SUPERVISOR] Restarting in {restart_delay} seconds\n")

                time.sleep(restart_delay)

    print("\n[ENGINE] Trading Engine stopped\n")





#_#_
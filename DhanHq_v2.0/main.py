# ============================================================
# MAIN TRADING ENGINE v3.5
# Production Orchestrator
# Supervisor Layer Integrated
# ============================================================

import asyncio
import signal
import time
import sys

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
# SAFE SPOT FETCH (ASYNC SAFE)
# ============================================================

async def wait_for_spot_price(engine, exchange, token, timeout=10):

    start = time.time()

    while True:

        price = engine.get_ltp_live(exchange, token)

        if price is not None:
            return price

        if time.time() - start > timeout:
            return None

        await asyncio.sleep(0.2)


# ============================================================
# ATM OPTION DISCOVERY
# ============================================================

async def discover_atm_option_pair(engine, registry, symbol, exchange):

    underlying_token = registry.get_token(exchange, symbol)

    if underlying_token is None:
        return None, None

    spot = await wait_for_spot_price(engine, exchange, underlying_token)

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
# RESOLVE PARENT TOKEN
# ============================================================

def resolve_parent_token(registry, exchange, symbol, parent_type):

    if parent_type == "FUT":

        futures = registry.get_futures(symbol)

        if futures:
            return futures[0].token

        return None

    return registry.get_token(exchange, symbol)


# ============================================================
# BUILD SIGNAL NODES
# ============================================================

def build_signal_nodes(engine, registry):

    nodes = []

    for config in STRATEGY_CONFIG:

        symbol = config["parent_symbol"]
        exchange = config["parent_exchange"]

        parent_type = config.get("parent_type", "IDX")

        token = resolve_parent_token(
            registry,
            exchange,
            symbol,
            parent_type
        )

        if token is None:
            continue

        node = InstrumentNode(
            engine,
            exchange,
            symbol,
            token
        )

        nodes.append(node)

    return nodes


# ============================================================
# BUILD TRADE MANAGERS
# ============================================================

async def build_trade_managers(engine, registry):

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

            ce_token, pe_token = await discover_atm_option_pair(
                engine,
                registry,
                parent_symbol,
                parent_exchange
            )

            if ce_token is None or pe_token is None:
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

async def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    engine = APIEngine()

    engine.market_data_map = {}

    registry = TokenRegistry()

    registry.load_master("data/instruments.csv")

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WebSocket connected\n")

    nodes = build_signal_nodes(engine, registry)

    for node in nodes:
        await node.initialize()
        node.start()

    engine.instrument_nodes = nodes

    trade_managers = await build_trade_managers(engine, registry)

    monitor = DisplayMonitor(engine, trade_managers, nodes)
    asyncio.create_task(asyncio.to_thread(monitor.start))

    for tm in trade_managers:

        publisher = SignalPublisher(

            parent_token=tm.parent_token,
            child_token=tm.child_token,

            ce_token=getattr(tm, "ce_token", None),
            pe_token=getattr(tm, "pe_token", None),

            product_type=tm.product_type
        )

        for node in nodes:

            if node.token == tm.parent_token:
                node.signal_engine.publisher = publisher

    tasks = []

    for node in nodes:
        tasks.extend(node.get_tasks())

    for tm in trade_managers:
        tasks.append(asyncio.create_task(tm.run()))

    await asyncio.gather(*tasks, return_exceptions=True)

    engine.shutdown()


# ============================================================
# SUPERVISOR LAYER
# ============================================================

def shutdown():

    print("\n[ENGINE] Shutdown requested\n")


if __name__ == "__main__":

    if len(sys.argv) > 1:

        DEBUG_CHART_SYMBOL = sys.argv[1]

        print(f"[DEBUG] Chart mode enabled → {DEBUG_CHART_SYMBOL}")

    restart_limit = 3
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




#_#_#_#_#_#_#_#_#_#_#_#_#_
# ============================================================
# MAIN TRADING ENGINE v6.1 (SURGICAL FIX)
# ATM Resolution Fixed | TOKEN_BUS Compatible | No Duplicates
# ============================================================

import time
import threading

from helper_wraper import APIEngine
from token_registry import TokenRegistry, TOKEN_BUS
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from display_monitor import DisplayMonitor


# ============================================================
#  STRATEGY CONFIG
# ============================================================

STRATEGY_CONFIG = [
    {
        "parent_IDX_Symbol" : "Nifty 50",
        "parent_IDX_Exchange": "NSE",
        "parent_IDX_Type" : "IDX",

        "parent_symbol": "NIFTY",        
        "parent_exchange": "NFO",      
        "parent_type": "FUTIDX",

        "child_exchange": "NFO",
        "product_type": "OPTIDX",
        "qty": int(65),
        "lot_size":int(2),

        "max_retry": 3,
        "strike_dist": 50,

        # runtime injected
        "ce_token": None,
        "pe_token": None
    },

    {
        "parent_IDX_Symbol" : "Nifty Bank",
        "parent_IDX_Exchange": "NSE",
        "parent_IDX_Type" : "IDX",

        "parent_symbol": "BANKNIFTY",        
        "parent_exchange": "NFO",      
        "parent_type": "FUTIDX",

        "child_exchange": "NFO",
        "product_type": "OPTIDX",
        "qty": int(30),
        "lot_size":int(4),

        "max_retry": 2,
        "strike_dist": 100,

        # runtime injected
        "ce_token": None,
        "pe_token": None
    },

    {
        "parent_IDX_Symbol" : "RELIANCE",
        "parent_IDX_Exchange": "NSE",
        "parent_IDX_Type" : "EQ",

        "parent_symbol": "RELIANCE",        
        "parent_exchange": "NFO",      
        "parent_type": "FUTSTK",


        "child_exchange": "NSE",
        "product_type": "EQ",
        "qty": int(5),
        "lot_size":int(1),

        "max_retry": 1,
        "strike_dist": None,
       
        # runtime injected
        "ce_token": None,
        "pe_token": None
    },

    {
        "parent_IDX_Symbol" : "RELIANCE",
        "parent_IDX_Exchange": "NSE",
        "parent_IDX_Type" : "EQ",

        "parent_symbol": "RELIANCE",        
        "parent_exchange": "NFO",      
        "parent_type": "FUTSTK",


        "child_exchange": "NFO",
        "product_type": "OPTSTK",
        "qty": int(500),
        "lot_size":int(1),

        "max_retry": 2,
        "strike_dist": None,
       
        # runtime injected
        "ce_token": None,
        "pe_token": None
    },
]


# ============================================================
# TOKEN RESOLUTION   
# ============================================================

def resolve_parent_token(registry, exchange, symbol, parent_type):

    # FUT placeholder retained (no change)
    if parent_type == "FUT":
        futures = registry.get_futures(symbol)
        if futures:
            return futures[0].token
        return None

    #  force dictionary lookup + TOKEN_BUS registration
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
# BUILD TRADE MANAGERS (NO GENERATOR CALLS HERE)
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

        #  use pre-resolved tokens ONLY
        if product_type == "OPT":

            ce_token = config.get("ce_token")
            pe_token = config.get("pe_token")

            if not ce_token or not pe_token:
                print(f"[ENGINE] Missing CE/PE tokens → {parent_symbol}")
                continue

            child_token = ce_token

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
            tm.ce_token = config["ce_token"]
            tm.pe_token = config["pe_token"]

        managers.append(tm)

    return managers


# ============================================================
# ENGINE BOOT
# ============================================================

def engine_bootloader():

    print("\n[ENGINE] Booting Trading Engine\n")

    engine = APIEngine()
    engine.market_data_map = {}

    registry = TokenRegistry(api=engine.api)
    registry.load_master()

    # --------------------------------------------------------
    # REGISTER + RESOLVE TOKENS
    # --------------------------------------------------------

    for config in STRATEGY_CONFIG:

        symbol = config["parent_symbol"]
        exchange = config["parent_exchange"]
        parent_type = config.get("parent_type", "IDX")

        try:
            #  forces TOKEN_BUS entry
            parent_token = resolve_parent_token(registry, exchange, symbol, parent_type)

            if parent_token is None:
                raise Exception("Parent token not found")

        except Exception as e:
            print(f"[ENGINE] Parent registration failed → {symbol} | {e}")
            continue

        # ----------------------------------------------------
        #  ATM RESOLUTION (ONLY ONCE HERE)
        # ----------------------------------------------------

        if config["product_type"] == "OPT":

            try:

                result = registry.register_atm_options(
                    engine,
                    config["parent_symbol"],
                    config["parent_exchange"],
                    config["child_exchange"],
                    config["strike_dist"]
                )

                (ce_symbol, ce_token), (pe_symbol, pe_token) = result

                config["ce_token"] = ce_token
                config["pe_token"] = pe_token

            except Exception as e:

                print(f"[ENGINE] ATM option registration failed → {symbol} | {e}")
                continue

    # --------------------------------------------------------
    # SUBSCRIBE
    # --------------------------------------------------------

    for inst in TOKEN_BUS:
        engine.subscribe(inst["exchange"], inst["token"])

    # --------------------------------------------------------
    # WS START
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
    # LOOP
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






#_#_#
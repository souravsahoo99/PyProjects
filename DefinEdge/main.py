# ============================================================
# MAIN TRADING ENGINE v8.1 (UNIFIED)
# Single File: Config + Resolver + Engine
# ============================================================

import time
import threading

from helper_wraper import APIEngine
from token_registry import TokenRegistry, TOKEN_BUS
from instrument_node import InstrumentNode
from trade_manager import TradeManager
from display_monitor import DisplayMonitor


# ============================================================
# STRATEGY CONFIG (LOCKED)
# ============================================================

STRATEGY_CONFIG = [
    {
        "parent_IDX_symbol" : "Nifty 50",
        "parent_IDX_exchange": "NSE",
        "parent_IDX_inst_type" : "IDX",

        "parent_symbol": "NIFTY",        
        "parent_exchange": "NFO",      
        "parent_inst_type": "FUTIDX",

        "child_exchange" : "NFO",
        "child_inst_type": "OPTIDX",
        "qty": int(65),
        "lot_size":int(2),

        "max_retry": 3,
        "strike_dist": 50,

        # runtime injected
        "ce_token": None,
        "pe_token": None
    },

    {
        "parent_IDX_symbol" : "Nifty Bank",
        "parent_IDX_exchange": "NSE",
        "parent_IDX_inst_type" : "IDX",

        "parent_symbol": "BANKNIFTY",        
        "parent_exchange": "NFO",      
        "parent_inst_type": "FUTIDX",

        "child_exchange" : "NFO",
        "child_inst_type": "OPTIDX",
        "qty": int(30),
        "lot_size":int(4),

        "max_retry": 2,
        "strike_dist": 100,

        # runtime injected
        "ce_token": None,
        "pe_token": None
    },

    {
        "parent_IDX_symbol" : "GOLDBEES",
        "parent_IDX_exchange": "NSE",
        "parent_IDX_inst_type" : "EQ",

        "parent_symbol": "GOLDM",        
        "parent_exchange": "MCX",      
        "parent_inst_type": "FUTCOM",


        "child_exchange": "NSE",
        "child_inst_type": "EQ",
        "qty": int(5),
        "lot_size":int(1),

        "max_retry": 2,
        "strike_dist": None,
       
        # runtime injected
        "ce_token": None,
        "pe_token": None
    },
]


# ============================================================
# RESOLUTION (CHECKPOINT 2.1)
# ============================================================

def resolve_config_tokens(engine, registry, config):

    # ---------------- IDX ----------------
    idx_symbol = registry.get_symbol_for_Index(
        config["parent_IDX_exchange"],
        config["parent_IDX_symbol"],
        config["parent_IDX_inst_type"]
    )

    registry.get_token(config["parent_IDX_exchange"], idx_symbol)

    # ---------------- PARENT ----------------
    inst = config["parent_inst_type"]

    if inst == "IDX":

        parent_symbol = registry.get_symbol_for_Index(
            config["parent_exchange"],
            config["parent_symbol"],
            inst
        )

    else:
        expiry = registry.get_nearest_expiry(
            config["parent_exchange"],
            config["parent_symbol"],
            inst
        )

        parent_symbol = registry.get_symbol_for_futures(
            config["parent_exchange"],
            config["parent_symbol"],
            inst,
            expiry
        )

    parent_token = registry.get_token(
        config["parent_exchange"],
        parent_symbol
    )

    config["parent_token"] = parent_token
    config["parent_trading_symbol"] = parent_symbol

    # ---------------- CHILD ----------------
    child_inst = config["child_inst_type"]

    # ===== OPTIONS =====
    if child_inst in ["OPTIDX", "OPTSTK", "OPTFUT"]:

        if config["strike_dist"]:

            result = registry.register_atm_options(
                engine,
                config["parent_IDX_symbol"],
                config["parent_IDX_exchange"],
                config["child_exchange"],
                config["strike_dist"]
            )

            (ce_sym, ce_tok), (pe_sym, pe_tok) = result

            config["ce_token"] = ce_tok
            config["pe_token"] = pe_tok

    # ===== EQ =====
    elif child_inst == "EQ":

        child_symbol = config["parent_IDX_symbol"]

        token = registry.get_token(
            config["child_exchange"],
            child_symbol
        )

        config["child_token"] = token


# ============================================================
# BUILD NODES
# ============================================================

def build_signal_nodes(engine):

    nodes = []

    for cfg in STRATEGY_CONFIG:

        token = cfg.get("parent_token")
        if not token:
            continue

        node = InstrumentNode(
            engine,
            cfg["parent_exchange"],
            cfg["parent_symbol"],
            token,
            instrument_type=cfg["parent_inst_type"],
            node_scope="PARENT"
        )

        nodes.append(node)

    return nodes


# ============================================================
# BUILD TRADE MANAGERS
# ============================================================

def build_trade_managers(engine):

    managers = []

    for cfg in STRATEGY_CONFIG:

        parent_token = cfg.get("parent_token")
        if not parent_token:
            continue

        child_token = parent_token

        if cfg["child_inst_type"] in ["OPTIDX", "OPTSTK", "OPTFUT"]:

            if not cfg.get("ce_token"):
                continue

            child_token = cfg["ce_token"]

        elif cfg["child_inst_type"] == "EQ":

            child_token = cfg.get("child_token")

        tm = TradeManager(
            engine=engine,
            parent_exchange=cfg["parent_exchange"],
            child_exchange=cfg["child_exchange"],
            signal_symbol=cfg["parent_symbol"],
            trading_symbol=cfg["parent_symbol"],
            parent_token=parent_token,
            child_token=child_token,
            product_type=cfg["child_inst_type"],
            qty=cfg["qty"],
            ws_ltp=None,
            rest_ltp=None,
            max_retry=cfg["max_retry"]
        )

        if cfg["child_inst_type"] in ["OPTIDX", "OPTSTK", "OPTFUT"]:
            tm.ce_token = cfg["ce_token"]
            tm.pe_token = cfg["pe_token"]

        managers.append(tm)

    return managers


# ============================================================
# ENGINE
# ============================================================

def engine_bootloader():

    print("\n[ENGINE] Booting...\n")

    engine = APIEngine()
    engine.market_data_map = {}

    registry = TokenRegistry(api=engine.api)
    registry.load_master()

    # -------- RESOLVE --------
    for cfg in STRATEGY_CONFIG:
        try:
            resolve_config_tokens(engine, registry, cfg)
        except Exception as e:
            print(f"[RESOLVE ERROR] {cfg['parent_symbol']} → {e}")

    # -------- SUBSCRIBE --------
    for inst in TOKEN_BUS:
        engine.subscribe(inst["exchange"], inst["token"])

    engine.start_ws()
    engine.wait_for_ws()

    print("[ENGINE] WS Connected\n")

    # -------- BUILD --------
    nodes = build_signal_nodes(engine)
    for n in nodes:
        n.initialize()
        n.start()

    managers = build_trade_managers(engine)

    monitor = DisplayMonitor(engine, managers, nodes)
    threading.Thread(target=monitor.start, daemon=True).start()

    for tm in managers:
        threading.Thread(target=tm.run, daemon=True).start()

    # -------- LOOP --------
    while True:
        time.sleep(1)


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    restart_limit = 4
    restart_count = 0

    restart_delay = 5

    while True:
        if restart_count <= restart_limit :     
            try:
                engine_bootloader()
                break
            except Exception as e:
                restart_count += 1            
                print(f"[CRASH] {e}")
                time.sleep(restart_delay)

        else:
            break




#_#_#_#_
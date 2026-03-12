# ============================================================
# TRADE MANAGER v2.8
# Production Grade Execution Engine (Mongo + Pointer Stable)
# Compatible with main.py + signal_engine.py
# ============================================================

import time
import asyncio
import json
import os

from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv

from dhanAPI_helper import Order
from signal_engine import SIGNALS


# ============================================================
# LOAD ENV
# ============================================================

dotenvfile = find_dotenv()
load_dotenv(dotenvfile)

CONNECTION_STR = os.getenv("CONNECTION_STRING")

mongo_client = MongoClient(CONNECTION_STR)

mongo_collection = mongo_client['AlgoBot']['TradeLogs']


# ============================================================
# TRADE MANAGER
# ============================================================

class TradeManager:

    def __init__(
        self,
        engine,
        parent_exchange,
        child_exchange,
        signal_symbol,
        trading_symbol,
        parent_token,
        child_token,
        product_type,
        qty,
        ws_ltp,
        rest_ltp,
        max_retry
    ):

        self.engine = engine

        self.parent_exchange = parent_exchange
        self.child_exchange = child_exchange

        self.signal_symbol = signal_symbol
        self.trading_symbol = trading_symbol

        self.parent_token = parent_token
        self.child_token = child_token

        self.product_type = product_type

        self.ce_token = None
        self.pe_token = None

        self.qty = qty

        self.signal_validity_seconds = 5

        self.max_retry = max_retry
        self.retry_count = 0

        self.strategy_name = None

        # ----------------------------------------------------
        # POINTER SYSTEM
        # ----------------------------------------------------

        self.state_dir = "state"
        os.makedirs(self.state_dir, exist_ok=True)

        self.pointer_file = f"{self.state_dir}/{self.signal_symbol}_manager.json"

        # ----------------------------------------------------
        # MONGO
        # ----------------------------------------------------

        self.mongo_collection = mongo_collection
        self.mongo_object_id = None

        # ----------------------------------------------------
        # TRADE DICT
        # ----------------------------------------------------

        self.trade = {

            "strategy_state": None,
            "manager_name": "TradeManager",

            "strategy_name": None,
            "side": None,

            "parentsignal_symbol": signal_symbol,

            "parent_exchange": parent_exchange,
            "child_exchange": child_exchange,

            "parent_token": parent_token,
            "child_token": child_token,

            "trading_symbol": trading_symbol,

            "qty": qty,
            "product_type": product_type,

            "entry_price": None,
            "entry_time": None,

            "exit_price": None,
            "exit_time": None,

            "stop_loss": -450,
            "target": 1000,
            "trailing_distance": 20,

            "net_pnl": 0,
            "max_pnl": 0,
            "min_pnl": 0,

            "retry_count": 0,
            "max_retry": max_retry,

            "mongo_object_id": None,

            "manager_start_time": time.time(),
            "last_update_time": None
        }


    # ========================================================
    # POINTER WRITE
    # ========================================================

    def _write_pointer(self):

        pointer_data = {

            "manager_name": self.trade["manager_name"],
            "parent_symbol": self.signal_symbol,
            "parent_token": self.parent_token,

            "mongo_object_id": str(self.mongo_object_id),

            "strategy_state": self.trade["strategy_state"],

            "child_token": self.child_token,
            "trading_symbol": self.trading_symbol,

            "timestamp": time.time()
        }

        with open(self.pointer_file, "w") as f:
            json.dump(pointer_data, f, indent=4)


    # ========================================================
    # MONGO INSERT
    # ========================================================

    def _mongo_insert(self):

        result = self.mongo_collection.insert_one(self.trade)

        self.mongo_object_id = result.inserted_id

        self.trade["mongo_object_id"] = str(self.mongo_object_id)

        self._write_pointer()


    # ========================================================
    # MONGO UPDATE
    # ========================================================

    def _mongo_update(self):

        if self.mongo_object_id is None:
            return

        self.trade["retry_count"] = self.retry_count
        self.trade["child_token"] = self.child_token
        self.trade["trading_symbol"] = self.trading_symbol

        self.trade["last_update_time"] = time.time()

        self.mongo_collection.update_one(
            {"_id": self.mongo_object_id},
            {"$set": self.trade}
        )

        self._write_pointer()


    # ========================================================
    # FETCH LTP
    # ========================================================

    def _get_ltp(self):

        price = self.engine.get_ltp_live(
            self.child_exchange,
            self.child_token
        )

        return price


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        side = signal.get("side")

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.trading_symbol} entry signal")

        self.trade["strategy_state"] = "ENTERING"
        self.trade["side"] = side
        self.trade["strategy_name"] = signal.get("strategy")

        order_side = "B" if side == "BUY" else "S"

        order = Order(
            buy_or_sell=order_side,
            product_type="C",
            exchange=self.child_exchange,
            tradingsymbol=self.trading_symbol,
            quantity=self.qty,
            price_type="MKT",
            price=0
        )

        ret = self.engine.api.place_order(order)

        if ret is None:
            print("[TRADE] Entry rejected")
            self.trade["strategy_state"] = None
            return

        ltp = self._get_ltp()

        if ltp is None:
            print("[TRADE] Entry price unavailable")
            self.trade["strategy_state"] = None
            return

        self.trade["entry_price"] = ltp
        self.trade["entry_time"] = time.time()

        self.trade["strategy_state"] = "ACTIVE"

        if self.mongo_object_id is None:
            self._mongo_insert()
        else:
            self._mongo_update()

        print(f"[TRADE] {self.trading_symbol} entered @ {ltp}")

        try:
            SIGNALS.remove(signal)
        except ValueError:
            pass


    # ========================================================
    # EXIT TRADE
    # ========================================================

    def exit_trade(self):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        print(f"[TRADE] Exit {self.trading_symbol}")

        side = self.trade["side"]

        exit_side = "S" if side == "BUY" else "B"

        order = Order(
            buy_or_sell=exit_side,
            product_type="C",
            exchange=self.child_exchange,
            tradingsymbol=self.trading_symbol,
            quantity=self.qty,
            price_type="MKT",
            price=0
        )

        ret = self.engine.api.place_order(order)

        if ret is None:
            print("[TRADE] Exit rejected")
            return

        ltp = self._get_ltp()

        pnl = None

        if ltp is not None:

            if side == "BUY":
                pnl = ltp - self.trade["entry_price"]
            else:
                pnl = self.trade["entry_price"] - ltp

        self.trade["exit_price"] = ltp
        self.trade["exit_time"] = time.time()

        self.trade["net_pnl"] = pnl
        self.trade["strategy_state"] = "EXITED"

        self.retry_count += 1

        self._mongo_update()

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")


    # ========================================================
    # ASYNC ENGINE LOOP
    # ========================================================

    async def run(self):

        while True:

            try:

                for signal in list(SIGNALS):

                    if signal.get("symbol") != self.signal_symbol:
                        continue

                    if self.trade["strategy_state"] is None:

                        self.enter_trade(signal)

                    elif self.trade["strategy_state"] == "ACTIVE":

                        price = self._get_ltp()

                        if price is None:
                            continue

                        entry = self.trade["entry_price"]

                        if self.trade["side"] == "BUY":
                            pnl = price - entry
                        else:
                            pnl = entry - price

                        if pnl <= self.trade["stop_loss"]:
                            self.exit_trade()

                        elif pnl >= self.trade["target"]:
                            self.exit_trade()

            except Exception as e:

                print("[TRADE MANAGER ERROR]", e)

            await asyncio.sleep(0.2)




#_#
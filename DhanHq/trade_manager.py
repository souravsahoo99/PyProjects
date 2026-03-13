# ============================================================
# TRADE MANAGER v3.0
# Production Grade Execution Engine
# ============================================================

import time
import asyncio
import json
import os

from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv

from dhanAPI_helper import Order
from signal_engine import SIGNALS, SIGNAL_LOCK


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
    # MONGO FUNCTIONS
    # ========================================================

    def _mongo_insert(self):

        result = self.mongo_collection.insert_one(self.trade)

        self.mongo_object_id = result.inserted_id

        self.trade["mongo_object_id"] = str(self.mongo_object_id)


    def _mongo_update(self):

        if self.mongo_object_id is None:
            return

        self.trade["retry_count"] = self.retry_count
        self.trade["last_update_time"] = time.time()

        self.mongo_collection.update_one(
            {"_id": self.mongo_object_id},
            {"$set": self.trade}
        )


    # ========================================================
    # FETCH LTP
    # ========================================================

    def _get_ltp(self):

        return self.engine.get_ltp_live(
            self.child_exchange,
            self.child_token
        )


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        # signal expiry check
        if time.time() - signal["signal_time"] > self.signal_validity_seconds:
            return

        side = signal.get("side")

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.trading_symbol} entry signal")

        self.trade["strategy_state"] = "ENTERING"
        self.trade["side"] = side
        self.trade["strategy_name"] = signal.get("strategy")

        txn = "BUY" if side == "BUY" else "SELL"

        order = Order(
            security_id=self.child_token,
            exchange_segment=self.child_exchange,
            transaction_type=txn,
            quantity=self.qty,
            order_type="MARKET",
            product_type="INTRADAY",
            price=0
        )

        ret = self.engine.api.Place_Order(order)

        if ret is None:

            print("[TRADE] Entry rejected")
            self.trade["strategy_state"] = None
            return

        ltp = self._get_ltp()

        if ltp is None:

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


    # ========================================================
    # EXIT TRADE
    # ========================================================

    def exit_trade(self):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        side = self.trade["side"]

        txn = "SELL" if side == "BUY" else "BUY"

        order = Order(
            security_id=self.child_token,
            exchange_segment=self.child_exchange,
            transaction_type=txn,
            quantity=self.qty,
            order_type="MARKET",
            product_type="INTRADAY",
            price=0
        )

        ret = self.engine.api.Place_Order(order)

        if ret is None:
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
    # MAIN LOOP
    # ========================================================

    async def run(self):

        while True:

            try:

                with SIGNAL_LOCK:
                    signals = list(SIGNALS)

                for signal in signals:

                    if signal.get("symbol") != self.signal_symbol:
                        continue

                    if self.trade["strategy_state"] is None:

                        self.enter_trade(signal)

                    elif self.trade["strategy_state"] == "ACTIVE":

                        price = self._get_ltp()

                        if price is None:
                            continue

                        entry = self.trade["entry_price"]

                        pnl = price - entry if self.trade["side"] == "BUY" else entry - price

                        if pnl <= self.trade["stop_loss"]:
                            self.exit_trade()

                        elif pnl >= self.trade["target"]:
                            self.exit_trade()

            except Exception as e:

                print("[TRADE MANAGER ERROR]", e)

            await asyncio.sleep(0.3)




#_#_#_
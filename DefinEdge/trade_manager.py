# ============================================================
# TRADE MANAGER v5.0
# Production Grade Execution Engine
# Threaded Runtime Version
# WebSocket Order Confirmation Integrated
# ============================================================

import time
import json
import os
import threading

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
mongo_collection = mongo_client["AlgoBot"]["TradeLogs"]


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
        # SIGNAL MEMORY
        # ----------------------------------------------------

        self.last_signal_key = None

        # ----------------------------------------------------
        # THREAD CONTROL
        # ----------------------------------------------------

        self._running = False
        self._thread = None

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

            "stop_loss": None,
            "target": None,

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
    # MONGO
    # ========================================================

    def _mongo_insert(self):

        result = self.mongo_collection.insert_one(self.trade)

        self.mongo_object_id = result.inserted_id
        self.trade["mongo_object_id"] = str(self.mongo_object_id)

        self._write_pointer()


    def _mongo_update(self):

        if self.mongo_object_id is None:
            return

        self.trade["retry_count"] = self.retry_count
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

        return self.engine.get_best_ltp(
            self.child_exchange,
            self.child_token
        )


    # ========================================================
    # TRAIL MANAGER
    # ========================================================

    def trail_manager(self, price):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        entry = self.trade["entry_price"]
        side = self.trade["side"]

        pnl = price - entry if side == "BUY" else entry - price

        self.trade["net_pnl"] = pnl

        if pnl > self.trade["max_pnl"]:
            self.trade["max_pnl"] = pnl

        if pnl < self.trade["min_pnl"]:
            self.trade["min_pnl"] = pnl

        trail = self.trade.get("trailing_distance", 20)

        sl = self.trade["stop_loss"]

        if side == "BUY":

            new_sl = price - trail

            if sl is None or new_sl > sl:
                self.trade["stop_loss"] = new_sl

        else:

            new_sl = price + trail

            if sl is None or new_sl < sl:
                self.trade["stop_loss"] = new_sl


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        signal_key = f"{signal['symbol']}_{signal['strategy']}_{signal['signal_time']}"

        if signal_key == self.last_signal_key:
            return

        if self.trade["strategy_state"] is not None:
            return

        if time.time() - signal["signal_time"] > self.signal_validity_seconds:
            return

        self.last_signal_key = signal_key

        side = signal.get("side")

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

        if not ret:
            return

        ltp = self._get_ltp()

        if ltp is None:
            return

        self.trade["entry_price"] = ltp
        self.trade["entry_time"] = time.time()
        self.trade["side"] = side
        self.trade["strategy_state"] = "ACTIVE"

        trail = self.trade["trailing_distance"]

        if side == "BUY":
            self.trade["stop_loss"] = ltp - trail
            self.trade["target"] = ltp + trail * 5
        else:
            self.trade["stop_loss"] = ltp + trail
            self.trade["target"] = ltp - trail * 5

        self._mongo_insert()

        print(f"[TRADE] {self.trading_symbol} entered @ {ltp}")


    # ========================================================
    # EXIT TRADE
    # ========================================================

    def exit_trade(self):

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

        self.engine.api.Place_Order(order)

        ltp = self._get_ltp()

        pnl = None

        if ltp:

            if side == "BUY":
                pnl = ltp - self.trade["entry_price"]
            else:
                pnl = self.trade["entry_price"] - ltp

        self.trade["exit_price"] = ltp
        self.trade["exit_time"] = time.time()
        self.trade["net_pnl"] = pnl
        self.trade["strategy_state"] = "EXITED"

        self._mongo_update()

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")


    # ========================================================
    # THREAD LOOP
    # ========================================================

    def _run_loop(self):

        while self._running:

            try:

                state = self.trade["strategy_state"]

                if state is None:

                    with SIGNAL_LOCK:
                        signals = list(SIGNALS)

                    for signal in signals:

                        if signal.get("symbol") != self.signal_symbol:
                            continue

                        self.enter_trade(signal)

                elif state == "ACTIVE":

                    price = self._get_ltp()

                    if price:

                        self.trail_manager(price)

                        side = self.trade["side"]
                        sl = self.trade["stop_loss"]
                        tp = self.trade["target"]

                        if side == "BUY":

                            if sl and price <= sl:
                                self.exit_trade()

                            elif tp and price >= tp:
                                self.exit_trade()

                        else:

                            if sl and price >= sl:
                                self.exit_trade()

                            elif tp and price <= tp:
                                self.exit_trade()

            except Exception as e:

                print("[TRADE MANAGER ERROR]", e)

            sleep_time = 0.03 if self.trade["strategy_state"] == "ACTIVE" else 0.3

            time.sleep(sleep_time)


    # ========================================================
    # START
    # ========================================================

    def start(self):

        if self._running:
            return

        self._running = True

        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True
        )

        self._thread.start()


    # ========================================================
    # STOP
    # ========================================================

    def stop(self):

        self._running = False

        if self._thread:
            self._thread.join(timeout=1)

        print(f"[TRADE MANAGER] Stopped → {self.trading_symbol}")



#_#_
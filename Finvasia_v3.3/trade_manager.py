# ============================================================
# TRADE MANAGER v2.4
# Production Grade Execution Engine (Refactored)
# ============================================================
import os
import time
import asyncio
from dateutil import parser
from datetime import datetime
from pymongo import MongoClient
from ShoonyaAPI_helper import Order
from signal_engine import SIGNALS
from dotenv import find_dotenv, load_dotenv

# ============================================================
#  LOADING... CREDENTIALS 
# ============================================================

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

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


        self.trade = {

            "strategy_state": None,
            "manager_name": "TradeManager",
            "strategy_name": None,
            "side": None,

            "entry_price": None,
            "entry_time": None,
            "entry_date": str(datetime.now().date()),
            
            "exit_price": None,
            "exit_time": None,
            "exit_date": str(datetime.now().date()),

            "stop_loss": -450,
            "target": 1000,
            "trailing_distance": 20,

            "net_pnl": 0,
            "max_pnl": 0,
            "min_pnl": 0,

            "parentsignal_symbol": signal_symbol,

            "parent_token": parent_token,
            "child_token": child_token,

            "qty": qty,
            "product_type": product_type
        }

        # ----------------------------------------------------
        # MONGO PLACEHOLDER
        # ----------------------------------------------------
        CONNECTION_STR = os.getenv("MONGO_STRING")
        mongo_client = MongoClient(CONNECTION_STR)
        mongoDB = mongo_client['AlgoBot']['TradeLogs']
        self.mongo_client = mongoDB


    # ========================================================
    #  MONGO INSERT & UPDATE
    # ========================================================

    def _mongo_insert(self):

        # TODO MongoDB insert
        mongo = self.mongo_client.insert_one(self.trade)


    def _mongo_update(self):

        # Major's DO _ MongoDB update
        pass


    # ========================================================
    # FETCH LTP
    # ========================================================

    def _get_ltp(self):

        price = self.engine.get_ltp_live(
            self.child_exchange,
            self.child_token
        )

        if price is None:
            return None
        else:
            return price


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        side = signal.get("side")

        if side is None:
            return

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.trading_symbol} entry signal")

        self.trade["strategy_state"] = "ENTERING"
        self.trade["side"] = side
        self.trade["strategy_name"] = signal.get("strategy")

        if side == "BUY":
            order_side = "B"
        elif side == "SELL":
            order_side = "S"
        else:
            return

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

        order = Order(
            buy_or_sell="S",
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

        if ltp is None:
            pnl = None
        else:
            pnl = ltp - self.trade["entry_price"]

        self.trade["exit_price"] = ltp
        self.trade["exit_time"] = time.time()
        self.trade["net_pnl"] = pnl

        self.trade["strategy_state"] = "EXITED"

        self.retry_count += 1

        self._mongo_update()

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")


    # ========================================================
    # ROUTE CHILD TOKEN
    # ========================================================

    def _route_child_token(self, parent_signal):

        parent_side = parent_signal.get("side")

        if parent_side is None:
            return

        if self.product_type == "OPT":

            if parent_side == "BUY":
                self.child_token = self.ce_token

            elif parent_side == "SELL":
                self.child_token = self.pe_token

        elif self.product_type in ["SPOT", "STOCK"]:
            self.child_token = self.parent_token


    # ========================================================
    # FIND SIGNAL PAIR
    # ========================================================

    def _find_signal_pair(self):

        now = time.time()

        parent_signal = None
        child_signal = None

        for signal in reversed(SIGNALS):

            if signal is None:
                continue

            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            token = signal.get("token")

            if token == self.parent_token:

                parent_signal = signal
                break

        if parent_signal is None:
            return None, None

        self._route_child_token(parent_signal)

        for signal in reversed(SIGNALS):

            if signal is None:
                continue

            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            token = signal.get("token")

            if token == self.child_token:

                child_signal = signal
                break

        return parent_signal, child_signal


    # ========================================================
    # MAIN LOOP
    # ========================================================

    async def run(self):

        self._mongo_insert()

        while True:

            state = self.trade.get("strategy_state")

            if state ==None:

                parent_signal, child_signal = self._find_signal_pair()

                if parent_signal is None:
                    await asyncio.sleep(0.5)
                    continue

                if child_signal is None:
                    await asyncio.sleep(0.2)
                    continue

                parent_time = parent_signal.get("signal_time")
                child_time = child_signal.get("signal_time")

                parent_side = parent_signal.get("side")
                child_side = child_signal.get("side")

                if parent_time <= child_time:

                    if (child_time - parent_time) <= self.signal_validity_seconds:

                        if parent_side == child_side:

                            self.enter_trade(child_signal)

                await asyncio.sleep(0.3)
                continue


            elif state == "ACTIVE":

                ltp = self._get_ltp()

                if ltp is None:
                    await asyncio.sleep(0.2)
                    continue

                entry = self.trade.get("entry_price")

                pnl = ltp - entry

                self.trade["net_pnl"] = pnl

                if pnl > self.trade["max_pnl"]:
                    self.trade["max_pnl"] = pnl

                elif pnl < self.trade["min_pnl"]:
                    self.trade["min_pnl"] = pnl

                stopLoss = self.trade.get("stop_loss")
                target_  = self.trade.get("target")
                current_time = datetime.now().time()
                if stopLoss != None and target_ != None:

                    if pnl <= stopLoss:

                        print("[TRADE] Stop loss hit")
                        self.exit_trade()

                    elif pnl >= target_:

                        print("[TRADE] Target hit")
                        self.exit_trade()

                    elif current_time >= parser.parse("15:00:00").time():

                        print("[TRADE] Time Exit")
                        self.exit_trade()

                    else:
                        pass

                await asyncio.sleep(0.2)
                continue


            elif state == "EXITED":

                if self.retry_count < self.max_retry:

                    print("[TRADE] Restarting manager for next trade")

                    self.trade["strategy_state"] = None
                    continue

                else:

                    print("[TRADE] Max retry reached. Trade_Manager Turning Off.")
                    break


            else:

                await asyncio.sleep(0.2)
                continue


# ============================================================
# PAPER TRADE MANAGER
# ============================================================

class PaperTradeManager(TradeManager):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.trade["manager_name"] = "PaperTradeManager"


    # ========================================================
    # ENTER TRADE (SIMULATED)
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        side = signal.get("side")

        if side not in ["BUY", "SELL"]:
            return

        print(f"[PAPER] {self.trading_symbol} entry signal")

        self.trade["strategy_state"] = "ACTIVE"
        self.trade["side"] = side
        self.trade["strategy_name"] = signal.get("strategy")

        ltp = self._get_ltp()

        if ltp is None:
            return

        self.trade["entry_price"] = ltp
        self.trade["entry_time"] = time.time()

        self._mongo_update()

        try:
            SIGNALS.remove(signal)
        except ValueError:
            pass


    # ========================================================
    # EXIT TRADE (SIMULATED)
    # ========================================================

    def exit_trade(self):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        print(f"[PAPER] Exit {self.trading_symbol}")

        ltp = self._get_ltp()

        if ltp is None:
            pnl = None
        else:
            pnl = ltp - self.trade["entry_price"]

        self.trade["exit_price"] = ltp
        self.trade["exit_time"] = time.time()
        self.trade["net_pnl"] = pnl

        self.trade["strategy_state"] = "EXITED"

        self.retry_count += 1

        self._mongo_update()

        print(f"[PAPER] Exit @ {ltp} | PnL {pnl}")







#_#_#_#_#_#_#_
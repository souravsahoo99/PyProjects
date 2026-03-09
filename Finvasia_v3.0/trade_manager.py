
# ============================================================
# TRADE MANAGER v2.2
# Product Grade Execution Engine
# ============================================================

import time
import asyncio

from ShoonyaAPI_helper import Order
from helper_wraper import get_best_ltp
from signal_engine import SIGNALS


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
        rest_ltp
    ):

        # ----------------------------------------------------
        # ENGINE
        # ----------------------------------------------------

        self.engine = engine

        # ----------------------------------------------------
        # EXCHANGES
        # ----------------------------------------------------

        self.parent_exchange = parent_exchange
        self.child_exchange = child_exchange

        # ----------------------------------------------------
        # SYMBOLS
        # ----------------------------------------------------

        self.signal_symbol = signal_symbol
        self.trading_symbol = trading_symbol

        # ----------------------------------------------------
        # TOKENS
        # ----------------------------------------------------

        self.parent_token = parent_token
        self.child_token = child_token

        # ----------------------------------------------------
        # PRODUCT TYPE
        # ----------------------------------------------------

        self.product_type = product_type   # FUT / OPT / SPOT

        # ----------------------------------------------------
        # OPTION TOKENS
        # ----------------------------------------------------

        self.ce_token = None
        self.pe_token = None

        # ----------------------------------------------------
        # TRADE PARAMETERS
        # ----------------------------------------------------

        self.qty = qty

        # ----------------------------------------------------
        # PRICE SOURCES
        # ----------------------------------------------------

        self.ws_ltp = ws_ltp
        self.rest_ltp = rest_ltp

        # ----------------------------------------------------
        # SIGNAL CONTROL
        # ----------------------------------------------------

        self.signal_validity_seconds = 5

        # ----------------------------------------------------
        # OPTIONAL TIME EXIT
        # ----------------------------------------------------

        self.time_exit = None

        # ----------------------------------------------------
        # STRATEGY NAME
        # ----------------------------------------------------

        self.strategy_name = None

        # ----------------------------------------------------
        # TRADE STATE OBJECT
        # ----------------------------------------------------

        self.trade = {

            "strategy_state": None,

            "entry_price": None,
            "entry_time": None,

            "stop_loss": None,
            "target": None,
            "trailing_distance": None,

            "net_pnl": 0,
            "max_pnl": 0,
            "min_pnl": 0
        }


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        side = signal["side"]

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.trading_symbol} entry signal")

        self.trade["strategy_state"] = "ENTERING"

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

        ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

        if ltp is None:

            print("[TRADE] Entry price unavailable")
            self.trade["strategy_state"] = None
            return

        self.trade["entry_price"] = ltp
        self.trade["entry_time"] = time.time()

        self.trade["strategy_state"] = "ACTIVE"

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

        ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

        pnl = None

        if ltp is not None:
            pnl = ltp - self.trade["entry_price"]

        self.trade["net_pnl"] = pnl

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")

        self.trade["strategy_state"] = "EXITED"


    # ========================================================
    # MAIN LOOP
    # ========================================================

    async def run(self):

        while True:

            state = self.trade["strategy_state"]

            # ------------------------------------------------
            # WAITING FOR SIGNAL
            # ------------------------------------------------

            if state is None:

                now = time.time()

                parent_signal = None
                child_signal = None

                # --------------------------------------------
                # SCAN GLOBAL SIGNAL LIST
                # --------------------------------------------

                for signal in reversed(SIGNALS):

                    # skip stale signals
                    if now - signal["signal_time"] > self.signal_validity_seconds:
                        continue

                    token = signal["token"]

                    # ----------------------------------------
                    # FIND PARENT SIGNAL
                    # ----------------------------------------

                    if token == self.parent_token and parent_signal is None:
                        parent_signal = signal

                    # ----------------------------------------
                    # FIND CHILD SIGNAL
                    # ----------------------------------------

                    if token == self.child_token and child_signal is None:
                        child_signal = signal

                # --------------------------------------------
                # PROCESS PARENT SIGNAL
                # --------------------------------------------

                if parent_signal:

                    parent_side = parent_signal["side"]

                    # store strategy name
                    self.strategy_name = parent_signal.get("strategy")

                    # ----------------------------------------
                    # PRODUCT TYPE ROUTING
                    # ----------------------------------------

                    if self.product_type == "OPT":

                        # BUY → CE
                        if parent_side == "BUY":
                            self.child_token = self.ce_token

                        # SELL → PE
                        elif parent_side == "SELL":
                            self.child_token = self.pe_token

                    elif self.product_type == "FUT":

                        # futures use provided token
                        pass

                    elif self.product_type in ["SPOT", "STOCK"]:

                        # trade same instrument
                        self.child_token = self.parent_token

                # --------------------------------------------
                # CHILD SIGNAL CONFIRMATION
                # --------------------------------------------

                if parent_signal and child_signal:

                    parent_time = parent_signal["signal_time"]
                    child_time = child_signal["signal_time"]

                    parent_side = parent_signal["side"]
                    child_side = child_signal["side"]

                    if parent_time <= child_time:

                        if (child_time - parent_time) <= self.signal_validity_seconds:

                            if parent_side == child_side:

                                self.enter_trade(child_signal)

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # ACTIVE TRADE MANAGEMENT
            # ------------------------------------------------

            if state == "ACTIVE":

                ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

                if ltp is None:
                    await asyncio.sleep(0.2)
                    continue

                entry = self.trade["entry_price"]

                pnl = ltp - entry

                self.trade["net_pnl"] = pnl

                if pnl > self.trade["max_pnl"]:
                    self.trade["max_pnl"] = pnl

                if pnl < self.trade["min_pnl"]:
                    self.trade["min_pnl"] = pnl

                sl = self.trade["stop_loss"]

                if sl is not None and ltp <= sl:

                    print("[TRADE] Stop loss hit")
                    self.exit_trade()
                    break

                target = self.trade["target"]

                if target is not None and ltp >= target:

                    print("[TRADE] Target hit")
                    self.exit_trade()
                    break

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # EXITED
            # ------------------------------------------------

            if state == "EXITED":
                break

            await asyncio.sleep(0.2)




#_#_#_#_#_#_
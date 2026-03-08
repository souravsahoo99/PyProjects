# ============================================================
# TRADE MANAGER (STATE MACHINE)
# Production Grade – Execution Pool Compatible
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
        exchange,
        symbol,
        parent_token,
        child_token,
        qty,
        ws_ltp,
        rest_ltp
    ):

        # ----------------------------------------------------
        # ENGINE
        # ----------------------------------------------------

        self.engine = engine
        self.exchange = exchange

        # ----------------------------------------------------
        # INSTRUMENT IDENTITY
        # ----------------------------------------------------

        self.symbol = symbol
        self.parent_token = parent_token
        self.child_token = child_token
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
        self.time_exit = None

        # ----------------------------------------------------
        # TRADE STATE MACHINE
        # ----------------------------------------------------

        self.trade = {

            "symbol": symbol,
            "parent_token": parent_token,
            "child_token": child_token,
            "qty": qty,

            "entry_price": None,
            "entry_time": None,
            "entry_side": None,

            "stop_loss": None,
            "target": None,
            "trailing_distance": None,

            "net_pnl": 0,
            "max_pnl": 0,
            "min_pnl": 0,

            "strategy_state": None
        }


    # ========================================================
    # FETCH VALID SIGNAL (DUAL TOKEN CONFIRMATION)
    # ========================================================

    def _get_valid_signal(self):

        now = time.time()

        parent_signal = None
        child_signal = None

        for signal in reversed(SIGNALS):

            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            token = signal["token"]

            if token == self.parent_token and parent_signal is None:
                parent_signal = signal

            if token == self.child_token and child_signal is None:
                child_signal = signal

            if parent_signal and child_signal:
                break

        if parent_signal and child_signal:

            parent_time = parent_signal["signal_time"]
            child_time = child_signal["signal_time"]

            if parent_time <= child_time:

                if (child_time - parent_time) <= self.signal_validity_seconds:
                    return child_signal

        return None


    # ========================================================
    # ENTER TRADE
    # ========================================================

    def enter_trade(self, signal):

        if self.trade["strategy_state"] is not None:
            return

        side = signal["side"]

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.symbol} entry signal received")

        self.trade["strategy_state"] = "ENTERING"

        order_side = "B" if side == "BUY" else "S"

        order = Order(

            buy_or_sell=order_side,
            product_type="C",
            exchange=self.exchange,
            tradingsymbol=self.symbol,
            quantity=self.qty,
            price_type="MKT",
            price=0
        )

        ret = self.engine.api.Place_Order(order)

        if ret is None:

            print("[TRADE] Entry rejected by broker")
            self.trade["strategy_state"] = None
            return

        ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

        if ltp is None:

            print("[TRADE] Entry price unavailable")
            self.trade["strategy_state"] = None
            return

        self.trade["entry_price"] = ltp
        self.trade["entry_time"] = time.time()
        self.trade["entry_side"] = side

        self.trade["strategy_state"] = "ACTIVE"

        print(f"[TRADE] {self.symbol} entered @ {ltp}")

        try:
            if signal in SIGNALS:
                SIGNALS.remove(signal)
        except Exception:
            pass


    # ========================================================
    # EXIT TRADE
    # ========================================================

    def exit_trade(self):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        print(f"[TRADE] Exiting {self.symbol}")

        entry_side = self.trade["entry_side"]

        exit_side = "S" if entry_side == "BUY" else "B"

        order = Order(

            buy_or_sell=exit_side,
            product_type="C",
            exchange=self.exchange,
            tradingsymbol=self.symbol,
            quantity=self.qty,
            price_type="MKT",
            price=0
        )

        ret = self.engine.api.Place_Order(order)

        if ret is None:

            print("[TRADE] Exit order rejected")
            return

        ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

        pnl = None

        if ltp is not None:

            entry = self.trade["entry_price"]

            if entry_side == "BUY":
                pnl = ltp - entry
            else:
                pnl = entry - ltp

        self.trade["net_pnl"] = pnl

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")

        self.trade["strategy_state"] = "EXITED"


    # ========================================================
    # TRAILING STOP LOSS
    # ========================================================

    def update_trailing_sl(self, ltp):

        distance = self.trade["trailing_distance"]

        if distance is None:
            return

        new_sl = ltp - distance

        if self.trade["stop_loss"] is None:

            self.trade["stop_loss"] = new_sl
            print("[TRADE] Initial trailing SL:", new_sl)

        elif new_sl > self.trade["stop_loss"]:

            self.trade["stop_loss"] = new_sl
            print("[TRADE] Trailing SL updated:", new_sl)


    # ========================================================
    # UPDATE PNL
    # ========================================================

    def update_pnl(self, ltp):

        entry = self.trade["entry_price"]

        if entry is None:
            return

        entry_side = self.trade["entry_side"]

        pnl = (ltp - entry) if entry_side == "BUY" else (entry - ltp)

        self.trade["net_pnl"] = pnl

        if pnl > self.trade["max_pnl"]:
            self.trade["max_pnl"] = pnl

        if pnl < self.trade["min_pnl"]:
            self.trade["min_pnl"] = pnl


    # ========================================================
    # TIME EXIT
    # ========================================================

    def check_time_exit(self):

        if self.time_exit is None:
            return False

        return time.time() >= self.time_exit


    # ========================================================
    # MAIN LOOP (STATE MACHINE)
    # ========================================================

    async def run(self):

        while True:

            state = self.trade["strategy_state"]

            if state is None:

                signal = self._get_valid_signal()

                if signal:
                    self.enter_trade(signal)

                await asyncio.sleep(0.2)
                continue


            if state == "ACTIVE":

                ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

                if ltp is None:

                    await asyncio.sleep(0.2)
                    continue

                self.update_pnl(ltp)

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

                self.update_trailing_sl(ltp)

                if self.check_time_exit():

                    print("[TRADE] Time exit triggered")
                    self.exit_trade()
                    break

                await asyncio.sleep(0.2)
                continue


            if state == "EXITED":
                break

            await asyncio.sleep(0.2)




#_#_#_#_#
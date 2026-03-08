# ============================================================
# TRADE MANAGER (STATE MACHINE)
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

    def __init__(self, engine, exchange, symbol, token, qty, ws_ltp, rest_ltp):

        self.engine = engine
        self.exchange = exchange

        self.symbol = symbol
        self.token = token
        self.qty = qty

        self.ws_ltp = ws_ltp
        self.rest_ltp = rest_ltp

        # signal validity window
        self.signal_validity_seconds = 5

        self.time_exit = None

        # ----------------------------------------------------
        # TRADE STATE
        # ----------------------------------------------------

        self.trade = {

            "symbol": symbol,
            "token": token,
            "qty": qty,

            "entry_price": None,
            "entry_time": None,

            "stop_loss": None,
            "target": None,
            "trailing_distance": None,

            "net_pnl": 0,
            "max_pnl": 0,
            "min_pnl": 0,

            "strategy_state": None
        }


    # ========================================================
    # FETCH VALID SIGNAL
    # ========================================================

    def _get_valid_signal(self):

        now = time.time()

        for signal in reversed(SIGNALS):

            # signal age check
            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            # token match
            if signal["token"] != self.token:
                continue

            return signal

        return None


    # ========================================================
    # ENTRY
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

        ret = self.engine.api.place_order(order)

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

        self.trade["strategy_state"] = "ACTIVE"

        print(f"[TRADE] {self.symbol} entered @ {ltp}")

        # remove consumed signal
        try:
            SIGNALS.remove(signal)
        except ValueError:
            pass


    # ========================================================
    # EXIT
    # ========================================================

    def exit_trade(self):

        if self.trade["strategy_state"] != "ACTIVE":
            return

        print(f"[TRADE] Exiting {self.symbol}")

        order = Order(

            buy_or_sell="S",
            product_type="C",
            exchange=self.exchange,
            tradingsymbol=self.symbol,
            quantity=self.qty,
            price_type="MKT",
            price=0
        )

        ret = self.engine.api.place_order(order)

        if ret is None:

            print("[TRADE] Exit order rejected")
            return

        ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

        pnl = None

        if ltp is not None:

            pnl = ltp - self.trade["entry_price"]

        self.trade["net_pnl"] = pnl

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")

        self.trade["strategy_state"] = "EXITED"


    # ========================================================
    # TRAILING STOP LOSS
    # ========================================================

    def update_trailing_sl(self, ltp):

        dist = self.trade["trailing_distance"]

        if dist is None:
            return

        new_sl = ltp - dist

        if self.trade["stop_loss"] is None:

            self.trade["stop_loss"] = new_sl
            print("[TRADE] Initial trailing SL:", new_sl)

        elif new_sl > self.trade["stop_loss"]:

            self.trade["stop_loss"] = new_sl
            print("[TRADE] Trailing SL updated:", new_sl)


    # ========================================================
    # PNL UPDATE
    # ========================================================

    def update_pnl(self, ltp):

        entry = self.trade["entry_price"]

        if entry is None:
            return

        pnl = ltp - entry

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
    # MAIN LOOP
    # ========================================================

    async def run(self):

        while True:

            state = self.trade["strategy_state"]

            # ------------------------------------------------
            # WAITING FOR SIGNAL
            # ------------------------------------------------

            if state is None:

                signal = self._get_valid_signal()

                if signal:
                    self.enter_trade(signal)

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # ACTIVE TRADE
            # ------------------------------------------------

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

                tgt = self.trade["target"]

                if tgt is not None and ltp >= tgt:

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


            # ------------------------------------------------
            # EXITED STATE
            # ------------------------------------------------

            if state == "EXITED":

                break

            await asyncio.sleep(0.2)


#_#_#_#
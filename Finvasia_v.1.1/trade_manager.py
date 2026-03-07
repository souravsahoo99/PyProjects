# ============================================================
# TRADE MANAGER (STATE MACHINE)
# ============================================================

import time
import asyncio

from ShoonyaAPI_helper import Order
from signal_engine import SIGNAL


# ============================================================
# TRADE MANAGER
# ============================================================

class TradeManager:
    """
    TradeManager is responsible for executing trades
    on ONE instrument while consuming signals from
    any signal pipeline.

    Example:
        Signal Source → NIFTY
        Execution Target → NIFTY CE
    """

    def __init__(self, engine, exchange, symbol, token, qty):

        self.engine = engine

        self.exchange = exchange
        self.symbol = symbol
        self.token = token
        self.qty = qty

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
    # FETCH BEST LTP
    # ========================================================

    def _get_ltp(self):

        ltp = self.engine.get_ltp_live(self.exchange, self.token)

        if ltp is None:
            ltp = self.engine.get_ltp_rest(self.exchange, self.token)

        return ltp


    # ========================================================
    # SIGNAL VALIDATION
    # ========================================================

    def _check_signal(self):

        if not SIGNAL["state"]:
            return False

        signal_time = SIGNAL["signal_time"]

        if signal_time is None:
            return False

        age = time.time() - signal_time

        if age > self.signal_validity_seconds:
            return False

        return True


    # ========================================================
    # ENTRY
    # ========================================================

    def enter_trade(self):

        if self.trade["strategy_state"] is not None:
            return

        side = SIGNAL["side"]

        if side not in ["BUY", "SELL"]:
            return

        print(f"[TRADE] {self.symbol} entry triggered by {SIGNAL['symbol']} signal")

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

        # correct API call
        ret = self.engine.api.Place_Order(order)

        if ret is None:

            print("[TRADE] Entry rejected by broker")
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

        print(f"[TRADE] {self.symbol} entered @ {ltp}")

        SIGNAL["state"] = False


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

        ret = self.engine.api.Place_Order(order)

        if ret is None:

            print("[TRADE] Exit order rejected")
            return

        ltp = self._get_ltp()

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

                if self._check_signal():
                    self.enter_trade()

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # ACTIVE TRADE
            # ------------------------------------------------

            if state == "ACTIVE":

                ltp = self._get_ltp()

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



##
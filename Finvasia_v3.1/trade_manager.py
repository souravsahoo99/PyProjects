# ============================================================
# TRADE MANAGER v2.3
# Production Grade Execution Engine (Readable Version)
# ============================================================

import time
import asyncio

from ShoonyaAPI_helper import Order
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

        self.product_type = product_type

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
        # TRADE STATE
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

        pnl = None

        if ltp is None:
            pnl = None

        elif ltp is not None:
            pnl = ltp - self.trade["entry_price"]

        else:
            pnl = None

        self.trade["net_pnl"] = pnl

        print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")

        self.trade["strategy_state"] = "EXITED"


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

            else:
                pass

        elif self.product_type == "FUT":

            pass

        elif self.product_type in ["SPOT", "STOCK"]:

            self.child_token = self.parent_token

        else:

            pass


    # ========================================================
    # FIND SIGNAL PAIR
    # ========================================================

    def _find_signal_pair(self):

        now = time.time()

        parent_signal = None
        child_signal = None

        # ----------------------------------------------------
        # FIND PARENT SIGNAL
        # ----------------------------------------------------

        for signal in reversed(SIGNALS):

            if signal is None:
                continue

            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            token = signal.get("token")

            if token == self.parent_token:

                parent_signal = signal
                break

            else:
                continue

        if parent_signal is None:
            return None, None

        # ----------------------------------------------------
        # ROUTE CHILD TOKEN
        # ----------------------------------------------------

        self._route_child_token(parent_signal)

        # ----------------------------------------------------
        # FIND CHILD SIGNAL
        # ----------------------------------------------------

        for signal in reversed(SIGNALS):

            if signal is None:
                continue

            if now - signal["signal_time"] > self.signal_validity_seconds:
                continue

            token = signal.get("token")

            if token == self.child_token:

                child_signal = signal
                break

            else:
                continue

        return parent_signal, child_signal


    # ========================================================
    # MAIN LOOP
    # ========================================================

    async def run(self):

        while True:

            state = self.trade.get("strategy_state")

            # ------------------------------------------------
            # WAITING FOR SIGNAL
            # ------------------------------------------------

            if state is None:

                parent_signal, child_signal = self._find_signal_pair()

                if parent_signal is None:
                    await asyncio.sleep(0.2)
                    continue

                if child_signal is None:
                    await asyncio.sleep(0.2)
                    continue

                parent_time = parent_signal.get("signal_time")
                child_time = child_signal.get("signal_time")

                parent_side = parent_signal.get("side")
                child_side = child_signal.get("side")

                if parent_time is None:
                    await asyncio.sleep(0.2)
                    continue

                if child_time is None:
                    await asyncio.sleep(0.2)
                    continue

                if parent_time <= child_time:

                    if (child_time - parent_time) <= self.signal_validity_seconds:

                        if parent_side == child_side:

                            self.strategy_name = parent_signal.get("strategy")

                            self.enter_trade(child_signal)

                        else:
                            pass

                    else:
                        pass

                else:
                    pass

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # ACTIVE TRADE MANAGEMENT
            # ------------------------------------------------

            elif state == "ACTIVE":

                ltp = self._get_ltp()

                if ltp is None:
                    await asyncio.sleep(0.2)
                    continue

                entry = self.trade.get("entry_price")

                if entry is None:
                    await asyncio.sleep(0.2)
                    continue

                pnl = ltp - entry

                self.trade["net_pnl"] = pnl

                if pnl > self.trade["max_pnl"]:
                    self.trade["max_pnl"] = pnl

                elif pnl < self.trade["min_pnl"]:
                    self.trade["min_pnl"] = pnl

                else:
                    pass

                sl = self.trade.get("stop_loss")

                if sl is not None:

                    if ltp <= sl:

                        print("[TRADE] Stop loss hit")
                        self.exit_trade()
                        break

                    else:
                        pass

                else:
                    pass

                target = self.trade.get("target")

                if target is not None:

                    if ltp >= target:

                        print("[TRADE] Target hit")
                        self.exit_trade()
                        break

                    else:
                        pass

                else:
                    pass

                await asyncio.sleep(0.2)
                continue


            # ------------------------------------------------
            # EXITED
            # ------------------------------------------------

            elif state == "EXITED":

                break

            else:

                await asyncio.sleep(0.2)
                continue





#_#_#_#_#_#_
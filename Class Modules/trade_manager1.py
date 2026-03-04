import time
import asyncio

from Shoonya_API_helper import Order
from helper_Wraper import get_best_ltp


# ===== TRADE MANAGER (STATE MACHINE) =====

class TradeManager:

    def __init__(self, engine, exchange, symbol, token, qty, ws_ltp, rest_ltp):

        self.engine = engine
        self.exchange = exchange
        self.ws_ltp = ws_ltp
        self.rest_ltp = rest_ltp

        # ----- TRADE STATE DICTIONARY -----

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
            "strategy_state": "NONE"
        }

        self.time_exit = None


    # ===== ENTRY =====

    def enter_trade(self):

        if self.trade["strategy_state"] == "NONE":

            print("[TRADE] Entering position")

            order = Order(
                buy_or_sell="B",
                product_type="C",
                exchange=self.exchange,
                tradingsymbol=self.trade["symbol"],
                quantity=self.trade["qty"],
                price_type="MKT",
                price=0
            )

            ret = self.engine.api.place_order(order)

            if ret is None:
                print("[TRADE] Entry order rejected")
                return
            else:
                pass

            ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

            if ltp is None:
                print("[TRADE] Entry failed (price unavailable)")
                return
            else:
                self.trade["entry_price"] = ltp
                self.trade["entry_time"] = time.time()
                self.trade["strategy_state"] = "ACTIVE"

                print(f"[TRADE] Entered at {ltp}")
                return

        else:

            print("[TRADE] Entry ignored. Strategy not in NONE state.")
            return


    # ===== EXIT =====

    def exit_trade(self):

        if self.trade["strategy_state"] == "ACTIVE":

            print("[TRADE] Exiting trade")

            order = Order(
                buy_or_sell="S",
                product_type="C",
                exchange=self.exchange,
                tradingsymbol=self.trade["symbol"],
                quantity=self.trade["qty"],
                price_type="MKT",
                price=0
            )

            ret = self.engine.api.place_order(order)

            if ret is None:
                print("[TRADE] Exit order rejected")
                return
            else:
                pass

            ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

            if ltp is None:

                print("[TRADE] Exit price unavailable")
                pnl = None

            else:

                pnl = ltp - self.trade["entry_price"]
                print(f"[TRADE] Exit @ {ltp} | PnL {pnl}")

            self.trade["net_pnl"] = pnl
            self.trade["strategy_state"] = "EXITED"

            return

        else:

            print("[TRADE] Exit ignored. No active position.")
            return


    # ===== TRAILING SL =====

    def update_trailing_sl(self, ltp):

        dist = self.trade["trailing_distance"]

        if dist is None:

            return

        else:

            new_sl = ltp - dist

            if self.trade["stop_loss"] is None:

                self.trade["stop_loss"] = new_sl
                print("[TRADE] Initial trailing SL:", new_sl)

            elif new_sl > self.trade["stop_loss"]:

                self.trade["stop_loss"] = new_sl
                print("[TRADE] Trailing SL updated:", new_sl)

            else:

                return


    # ===== PNL UPDATE =====

    def update_pnl(self, ltp):

        entry = self.trade["entry_price"]

        if entry is None:

            return

        else:

            pnl = ltp - entry
            self.trade["net_pnl"] = pnl

            if pnl > self.trade["max_pnl"]:

                self.trade["max_pnl"] = pnl

            else:

                pass

            if pnl < self.trade["min_pnl"]:

                self.trade["min_pnl"] = pnl

            else:

                pass

            return


    # ===== TIME EXIT =====

    def check_time_exit(self):

        if self.time_exit is None:

            return False

        else:

            now = time.time()

            if now >= self.time_exit:

                return True

            else:

                return False


    # ===== MAIN LOOP =====

    async def run(self):

        while True:

            state = self.trade["strategy_state"]
            ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

            if ltp is None:

                await asyncio.sleep(0.5)
                continue

            else:

                pass


            # ----- STATE : NONE -----

            if state == "NONE":

                pass


            # ----- STATE : ACTIVE -----

            elif state == "ACTIVE":

                self.update_pnl(ltp)

                sl = self.trade["stop_loss"]

                if sl is not None and ltp <= sl:

                    print("[TRADE] Stop loss hit")
                    self.exit_trade()
                    break

                else:

                    pass


                tgt = self.trade["target"]

                if tgt is not None and ltp >= tgt:

                    print("[TRADE] Target hit")
                    self.exit_trade()
                    break

                else:

                    pass


                self.update_trailing_sl(ltp)


                if self.check_time_exit():

                    print("[TRADE] Time exit triggered")
                    self.exit_trade()
                    break

                else:

                    pass


            # ----- STATE : EXITED -----

            elif state == "EXITED":

                break

            else:

                pass


            await asyncio.sleep(0.5)



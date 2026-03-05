import os
import time
import asyncio

from ShoonyaAPI_helper import Order
from helper_wraper import ShoonyaEngine 
from helper_wraper import get_best_ltp , cred
from dotenv import find_dotenv, load_dotenv
from tamingnifty import utils as util

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

# ==== Notification Management ====
slack_token = os.getenv("SLACK_TOKEN")
slack_client = util.get_slack_client(token=slack_token) 

# util.notify("Your Notification Message Comes here",slack_channel="pibot",slack_client=slack_client)


# ============================================================
# TRADE MANAGER (STATE MACHINE)
# ============================================================

class TradeManager:

    def __init__(self, engine, exchange, symbol, token, qty, ws_ltp, rest_ltp):

        self.engine = engine
        self.exchange = exchange
        self.ws_ltp = ws_ltp
        self.rest_ltp = rest_ltp
        self.time_exit = None              # time value comes here as 3PM exit or 15 minutes after entry etc. (optional)
        # -------------------------
        # TRADE STATE DICTIONARY
        # -------------------------

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

        


    # ============================================================
    #   ENTRY
    # ============================================================

    def enter_trade(self):

        if self.trade["strategy_state"] == None:

            # entry lock
            self.trade["strategy_state"] = "ENTERING"

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

                print("[TRADE] Entry order rejected by broker")

                # revert state
                self.trade["strategy_state"] = None

                return

            else:

                pass


            ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

            if ltp is None:

                print("[TRADE] Entry failed (price unavailable)")

                # revert state
                self.trade["strategy_state"] = None

                return

            else:

                self.trade["entry_price"] = ltp
                self.trade["entry_time"] = time.time()
                self.trade["strategy_state"] = "ACTIVE"

                print(f"[TRADE] Entered at {ltp}")
                util.notify(f"Entered trade for {self.trade['symbol']} at {ltp}", slack_channel="pibot", slack_client=slack_client)

                return 

        else:

            print("[TRADE] Entry ignored. Strategy already active.")

            return


    # ============================================================
    #   EXIT
    # ============================================================

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

                print("[TRADE] Exit order rejected by broker")

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
                util.notify(f"Exited trade for {self.trade['symbol']} at {ltp} | PnL {pnl}", slack_channel="pibot", slack_client=slack_client)


            self.trade["net_pnl"] = pnl
            self.trade["strategy_state"] = "EXITED"

            return

        else:

            print("[TRADE] Exit ignored. No active position.")

            return


    # ============================================================
    #   TRAILING SL
    # ============================================================

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


    # ============================================================
    #    PNL UPDATE
    # ============================================================

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


    # ============================================================
    #    TIME EXIT
    # ============================================================

    def check_time_exit(self):

        if self.time_exit is None:

            return False

        else:

            now = time.time()

            if now >= self.time_exit:

                return True

            else:

                return False


    # ============================================================
    #    MAIN LOOP ('STATE' DRIVEN)
    # ============================================================

    async def run(self):

        while True:

            state = self.trade["strategy_state"]


            # ------------------------------------------------
            # "STATE" : NONE
            # ------------------------------------------------

            if state == None:

                # waiting for strategy signal injection

                await asyncio.sleep(0.2)

                continue


            # ------------------------------------------------
            # "STATE" : ENTERING
            # ------------------------------------------------

            elif state == "ENTERING":

                # waiting for entry confirmation

                await asyncio.sleep(0.2)

                continue


            # ------------------------------------------------
            # "STATE" : ACTIVE
            # ------------------------------------------------

            elif state == "ACTIVE":

                ltp = get_best_ltp(self.ws_ltp, self.rest_ltp)

                if ltp is None:

                    await asyncio.sleep(0.2)

                    continue

                else:

                    pass


                self.update_pnl(ltp)


                sl = self.trade["stop_loss"]

                if sl is not None and ltp <= sl:

                    print("[TRADE] Stop loss hit")

                    self.exit_trade()
                    util.notify(f"Stop loss hit for {self.trade['symbol']} at {ltp}", slack_channel="pibot", slack_client=slack_client)

                    break

                else:

                    pass


                tgt = self.trade["target"]

                if tgt is not None and ltp >= tgt:

                    print("[TRADE] Target hit")

                    self.exit_trade()
                    util.notify(f"Target hit for {self.trade['symbol']} at {ltp}", slack_channel="pibot", slack_client=slack_client)

                    break

                else:

                    pass


                self.update_trailing_sl(ltp)


                if self.check_time_exit():

                    print("[TRADE] Time exit triggered")

                    self.exit_trade()
                    util.notify(f"Time exit triggered for {self.trade['symbol']} at {ltp}", slack_channel="pibot", slack_client=slack_client)

                    break

                else:

                    pass


                await asyncio.sleep(0.2)

                continue


            # ---------------------------------------------------------------------
            # "STATE" : EXITED  => Shutdown of while loop if missed in ACTIVE state
            # ---------------------------------------------------------------------

            elif state == "EXITED":

                break
            

#####################################################################################

# IMP: create an instance of the wrapper class and then invoke methods on it
api = ShoonyaEngine(credentials=cred)
if api != None:
    print(api)
    util.notify("API Login successful", slack_channel="pibot", slack_client=slack_client)
else:
    print("Login failed")


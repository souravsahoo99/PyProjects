# ============================================================
# DEFINEEDGE API HELPER v2.2
# Production Broker Adapter
# Engine Compatible
# WebSocket Hardened
# REST Tick LTP Fallback Added
# ============================================================

from integrate import ConnectToIntegrate, IntegrateOrders, IntegrateData, IntegrateWebSocket

import os
import io
import threading
import logging
import pyotp
import pandas as pd
import requests
import zipfile
from datetime import datetime
from dotenv import find_dotenv, load_dotenv

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

logger = logging.getLogger(__name__)


# ============================================================
# ORDER OBJECT (Engine Compatible)
# ============================================================

class Order:

    def __init__(
        self,
        security_id: str,
        exchange_segment,
        transaction_type,
        quantity: int,
        order_type,
        product_type,
        price: float = 0.0
    ):
        self.security_id = security_id
        self.exchange_segment = exchange_segment
        self.transaction_type = transaction_type
        self.quantity = quantity
        self.order_type = order_type
        self.product_type = product_type
        self.price = price


# ============================================================
# DEFINEEDGE API CLASS
# ============================================================

class EdgeApi:

    def __init__(self, api_token: str, api_secret: str):

        self.api_token = api_token
        self.api_secret = api_secret

        self.conn = self._login()

        # ----------------------------------------------------
        # REST MODULES
        # ----------------------------------------------------
        self.data = IntegrateData(self.conn)
        self.orders = IntegrateOrders(self.conn)

        # ----------------------------------------------------
        # WEBSOCKET MODULE
        # ----------------------------------------------------

        self.ws = IntegrateWebSocket(self.conn)

        # ----------------------------------------------------
        # INTERNAL STATE
        # ----------------------------------------------------

        self._ws_running = False

        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._order_buffer = {}
        self._order_lock = threading.Lock()

        self._subscribed = set()

        # ----------------------------------------------------
        # BIND CALLBACKS
        # ----------------------------------------------------

        self.ws.on_tick_update = self._on_tick_update
        self.ws.on_order_update = self._on_order_update
        self.ws.on_open = self._on_ws_open
        self.ws.on_close = self._on_ws_close
        self.ws.on_error = self._on_ws_error


    # ========================================================
    # LOGIN (SESSION AWARE)
    # ========================================================

    def _login(self):

        conn = ConnectToIntegrate()

        try:

            uid = os.environ["INTEGRATE_UID"]
            actid = os.environ["INTEGRATE_ACTID"]
            api_session = os.environ["INTEGRATE_API_SESSION_KEY"]
            ws_session = os.environ["INTEGRATE_WS_SESSION_KEY"]

            conn.set_session_keys(uid, actid, api_session, ws_session)

            logger.info("Reusing existing Integrate session.")

        except KeyError:

            totp_secret = os.getenv("EDGE_TOTP_SECRET")
            totp_ = pyotp.TOTP(totp_secret).now()

            conn.login(
                api_token=self.api_token,
                api_secret=self.api_secret,
                totp=totp_
            )

            uid, actid, api_session, ws_session = conn.get_session_keys()

            os.environ["INTEGRATE_UID"] = uid
            os.environ["INTEGRATE_ACTID"] = actid
            os.environ["INTEGRATE_API_SESSION_KEY"] = api_session
            os.environ["INTEGRATE_WS_SESSION_KEY"] = ws_session

            logger.info("New Integrate login successful.")

        return conn


    # ========================================================
    # WEBSOCKET CALLBACKS
    # ========================================================

    def _on_ws_open(self, iws):

        logger.info("DefineEdge WebSocket connected")

        try:
            iws.login()
        except Exception as e:
            logger.error(f"WS login error: {e}")


    def _on_ws_close(self, iws, code, reason):

        logger.warning(f"WebSocket closed: {code} {reason}")
        self._ws_running = False


    def _on_ws_error(self, iws, code, reason):

        logger.error(f"WebSocket error: {code} {reason}")


    # --------------------------------------------------------
    # TICK UPDATE
    # --------------------------------------------------------

    def _on_tick_update(self, iws, tick):

        try:

            exchange = tick.get("e") or "NSE"
            token = tick.get("tk")

            if token is None:
                return

            key = f"{exchange}|{token}"

            price = tick.get("lp") or tick.get("ltp")

            if price is None:
                return

            try:
                price = float(price)
            except Exception:
                return

            tick["lp"] = price

            with self._tick_lock:
                self._tick_cache[key] = tick

        except Exception as e:

            logger.error(f"Tick handler error: {e}")


    # --------------------------------------------------------
    # ORDER UPDATE
    # --------------------------------------------------------

    def _on_order_update(self, iws, order):

        try:

            order_id = order.get("orderId") or order.get("norenordno")
            status = order.get("status")

            if order_id is None or status is None:
                return

            with self._order_lock:
                self._order_buffer[str(order_id)] = str(status).upper()

        except Exception as e:

            logger.error(f"Order update handler error: {e}")


    # ========================================================
    # ENGINE COMPATIBILITY METHODS
    # ========================================================

    def Place_Order(self, order: Order):

        return self.orders.place_order(
            exchange=order.exchange_segment,
            order_type=order.transaction_type,
            price=order.price,
            price_type="MARKET",
            product_type=order.product_type,
            quantity=order.quantity,
            tradingsymbol=order.security_id
        )


    def Modify_Order(
        self,
        order_id,
        order_type,
        leg_name,
        quantity,
        price,
        trigger_price,
        disclosed_quantity,
        validity
    ):

        return self.orders.modify_order(
            exchange="NSE",
            order_id=order_id,
            order_type=order_type,
            price=price,
            price_type="LIMIT",
            product_type="INTRADAY",
            quantity=quantity,
            tradingsymbol=leg_name,
            trigger_price=trigger_price,
            disclosed_quantity=disclosed_quantity,
            validity=validity
        )


    def Cancel_Order(self, order_id):

        return self.orders.cancel_order(order_id)


    def Get_Positions(self):

        return self.orders.positions()


    def Get_Orderbook(self):

        return self.orders.orders()


    def Get_TradeBook(self):

        return self.orders.trades()


    def Get_Order_By_ID(self, order_id):

        return self.orders.order(order_id)


    # ========================================================
    # MARKET DATA (REST)
    # ========================================================

    def Get_LTP(self, trading_symbol, exchange):

        return self.data.quotes(exchange, trading_symbol)


    def Get_Intraday_Data(
        self,
        trading_symbol,
        exchange,
        timeframe,
        start,
        end
    ):

        if timeframe == "min":
            tf = self.conn.TIMEFRAME_TYPE_MIN
        elif timeframe == "day":
            tf = self.conn.TIMEFRAME_TYPE_DAY
        else:
            tf = timeframe

        return self.data.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=tf,
            start=start,
            end=end
        )


    def Get_Daily_Data(
        self,
        trading_symbol,
        exchange,
        start,
        end
    ):

        return self.data.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=self.conn.TIMEFRAME_TYPE_DAY,
            start=start,
            end=end
        )


    # ========================================================
    # REST TICK DATA (LTP FALLBACK)
    # ========================================================

    def Get_Tick_Data(
        self,
        trading_symbol,
        exchange,
        start,
        end
    ):

        try:

            ticks = self.data.historical_data(
                exchange=exchange,
                trading_symbol=trading_symbol,
                timeframe=self.conn.TIMEFRAME_TYPE_TICK,
                start=start,
                end=end
            )

            last_tick = None

            for t in ticks:
                last_tick = t

            if last_tick is None:
                return None

            return last_tick.get("ltp")

        except Exception as e:

            logger.error(f"Tick REST fetch error: {e}")

            return None


    # ========================================================
    # WEBSOCKET CONTROL
    # ========================================================

    def Start_Websocket(self):

        if self._ws_running:
            return

        try:

            self.ws.connect(daemonize=True)

            self._ws_running = True

        except Exception as e:

            logger.error(f"WebSocket start error: {e}")
            self._ws_running = False


    def Subscribe_inst(self, tokens):

        if not tokens:
            return

        try:

            self.ws.subscribe(
                self.conn.SUBSCRIPTION_TYPE_TICK,
                tokens
            )

            for token in tokens:
                self._subscribed.add(token)

        except Exception as e:

            logger.error(f"Subscribe error: {e}")


    def Unsubscribe_inst(self, tokens):

        if not tokens:
            return

        try:

            self.ws.unsubscribe(
                self.conn.SUBSCRIPTION_TYPE_TICK,
                tokens
            )

            for token in tokens:
                self._subscribed.discard(token)

        except Exception as e:

            logger.error(f"Unsubscribe error: {e}")


    def Close_Websocket(self):

        try:

            self._ws_running = False
            self.ws.close()

        except Exception as e:

            logger.error(f"WebSocket close error: {e}")


    # ========================================================
    # ORDER STREAM
    # ========================================================

    def Start_Order_Stream(self):

        try:

            self.ws.subscribe(
                self.conn.SUBSCRIPTION_TYPE_ORDER
            )

        except Exception as e:

            logger.error(f"Order stream subscribe error: {e}")


    # ========================================================
    # MASTER INSTRUMENT ZIP (HTTP)
    # ========================================================

    def download_master_zip(
        self,
        url="https://app.definedgesecurities.com/public/allmaster.zip"
    ):

        response = requests.get(url)

        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:

            name = z.namelist()[0]

            with z.open(name) as f:

                df = pd.read_csv(f, header=None, low_memory=False)

        return df


# ======= Download Master File ==========

def Load_Master(url="https://app.definedgesecurities.com/public/allmaster.zip"):

    response = requests.get(url)

    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:

        name = z.namelist()[0]

        with z.open(name) as f:

            df = pd.read_csv(f, header=None, low_memory=False)

    return df



#
# ============================================================
# EDGE API HELPER TEST TEMPLATE
# ============================================================

if __name__ == "__main__":

    print("\n===== EDGE API HELPER TEST START =====\n")

    # --------------------------------------------------------
    # INIT API
    # --------------------------------------------------------

    api_token = os.getenv("EDGE_API_TOKEN")
    api_secret = os.getenv("EDGE_API_SECRET")

    api = EdgeApi(api_token, api_secret)

    print("Login successful\n")


    # --------------------------------------------------------
    # TEST LTP REST
    # --------------------------------------------------------

    print("Testing LTP REST...")

    try:

        ltp = api.Get_LTP(
            trading_symbol="NIFTY-I",
            exchange="NSE"
        )

        print("LTP Response:", ltp)

    except Exception as e:

        print("LTP ERROR:", e)


    # --------------------------------------------------------
    # TEST HISTORICAL MINUTE
    # --------------------------------------------------------

    print("\nTesting Intraday Data...")

    try:

        data = api.Get_Intraday_Data(
            trading_symbol="NIFTY-I",
            exchange="NSE",
            timeframe="min",
            start=None,
            end=None
        )

        count = 0

        for row in data:

            print(row)

            count += 1

            if count >= 5:
                break

    except Exception as e:

        print("Historical ERROR:", e)


    # --------------------------------------------------------
    # TEST DAILY DATA
    # --------------------------------------------------------

    print("\nTesting Daily Data...")

    try:

        data = api.Get_Daily_Data(
            trading_symbol="NIFTY-I",
            exchange="NSE",
            start=None,
            end=None
        )

        for i,row in enumerate(data):

            print(row)

            if i >= 3:
                break

    except Exception as e:

        print("Daily ERROR:", e)


    # --------------------------------------------------------
    # TEST REST TICK DATA
    # --------------------------------------------------------

    print("\nTesting REST Tick Data...")

    try:

        tick = api.Get_Tick_Data(
            trading_symbol="NIFTY-I",
            exchange="NSE",
            start=None,
            end=None
        )

        print("Tick LTP:", tick)

    except Exception as e:

        print("Tick REST ERROR:", e)


    # --------------------------------------------------------
    # TEST MASTER FILE DOWNLOAD
    # --------------------------------------------------------

    print("\nTesting Master Instrument Download...")

    try:

        df = api.download_master_zip()

        print("Master rows:", len(df))

    except Exception as e:

        print("Master download ERROR:", e)


    print("\n===== EDGE API HELPER TEST END =====") 




#_
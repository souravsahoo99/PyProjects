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
from typing import Any
from datetime import datetime
from dotenv import find_dotenv, load_dotenv ,set_key

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

api_token = os.getenv("EDGE_API_TOKEN")
api_secret = os.getenv("EDGE_API_SECRET")
totp_secret = os.getenv("EDGE_TOTP_SECRET")

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

    def __init__(self, api_Token: str, api_Secret: str):

        self.api_token = api_Token
        self.api_secret = api_Secret

        self.api_session_key=None
        self.ws_session_key=None

        self.c2i= None

        self.ic = None
        self.io = None
        self.iws= None

        self._login()

        self._integrateData(self.c2i)
        self._integrateOrders(self.c2i)
        self._iWebsocket(self.c2i)

        # ----------------------------------------------------
        # INTERNAL STATE
        # ----------------------------------------------------

        self._ws_running = False

        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._order_buffer = {}
        self._order_lock = threading.Lock()

        self._subscribed = set()


    # ========================================================
    # LOGIN (SESSION AWARE)
    # ========================================================

    def _login(self):

        c2i = ConnectToIntegrate()
        Totp = pyotp.TOTP(totp_secret).now()

        c2i.login(
            api_token=self.api_token,
            api_secret=self.api_secret,
            totp=Totp ,
        )

        self.c2i=c2i
        self.api_session_key=c2i.api_session_key
        self.ws_session_key=c2i.ws_session_key

        print(f"\nAPI Session Key: {c2i.api_session_key}\nWS Session Key: {c2i.ws_session_key}")


    def _integrateData(self, c2i):

        ic = IntegrateData(c2i)

        self.ic = ic

    def _integrateOrders(self, c2i):

        io = IntegrateOrders(c2i)

        self.io = io

    def _iWebsocket(self, c2i):
        
        iws = IntegrateWebSocket(c2i)

        iws.on_open = self._on_ws_open
        iws.on_close = self._on_ws_close
        iws.on_error = self._on_ws_error
        iws.on_tick_update = self._on_tick_update
        iws.on_order_update = self._on_order_update
        
        self.iws = iws
          

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

        return self.io.place_order(
            exchange=order.exchange_segment,
            order_type=order.transaction_type,
            price=order.price,
            price_type="MARKET",
            product_type=order.product_type,
            quantity=order.quantity,
            tradingsymbol=order.security_id
        )


    def Modify_Order(self,order_id,order_type,leg_name,quantity,price,trigger_price,disclosed_quantity,validity):

        return self.io.modify_order(
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

        return self.io.cancel_order(order_id)


    def Get_Positions(self):

        return self.io.positions()


    def Get_Orderbook(self):

        return self.io.orders()


    def Get_TradeBook(self):

        return self.io.trades()


    def Get_Order_By_ID(self, order_id):

        return self.io.order(order_id)


    # ========================================================
    # MARKET DATA (REST)
    # ========================================================

    def Get_LTP(self, exchange, trading_symbol):

        return self.ic.quotes(exchange, trading_symbol)


    def Get_Intraday_Data(self,exchange,trading_symbol,timeframe,start,end):

        if timeframe == "min":
            tf = self.c2i.TIMEFRAME_TYPE_MIN
        elif timeframe == "day":
            tf = self.c2i.TIMEFRAME_TYPE_DAY
        else:
            tf = self.c2i.TIMEFRAME_TYPE_TICK

        return self.ic.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=tf,
            start=start,
            end=end
        )


    def Get_Daily_Data(self,exchange,trading_symbol,start,end):

        return self.ic.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=self.c2i.TIMEFRAME_TYPE_DAY,
            start=start,
            end=end
        )


    # ========================================================
    # REST TICK DATA (LTP FALLBACK)
    # ========================================================

    def Get_Tick_Data(self,exchange,trading_symbol,start,end):


        ticks = self.ic.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=self.c2i.TIMEFRAME_TYPE_TICK,
            start=start,
            end=end
            )

        return ticks


    # ========================================================
    # WEBSOCKET CONTROL
    # ========================================================

    def Start_Websocket(self):

        if self._ws_running:
            return

        try:

            self.iws.connect(daemonize=True)

            self._ws_running = True

        except Exception as e:

            logger.error(f"WebSocket start error: {e}")
            self._ws_running = False


    def Subscribe_inst(self, tokens):

        if not tokens:
            return

        try:

            self.iws.subscribe(
                self.c2i.SUBSCRIPTION_TYPE_TICK,
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

            self.iws.unsubscribe(
                self.c2i.SUBSCRIPTION_TYPE_TICK,
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

            self.iws.subscribe(
                self.c2i.SUBSCRIPTION_TYPE_ORDER
            )

        except Exception as e:

            logger.error(f"Order stream subscribe error: {e}")


    # ========================================================
    # MASTER INSTRUMENT ZIP (HTTP)
    # ========================================================

    def download_master_zip(self,url="https://app.definedgesecurities.com/public/allmaster.zip"):

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



#_#_#
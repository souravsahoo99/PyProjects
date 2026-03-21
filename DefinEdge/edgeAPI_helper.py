# ============================================================
# DEFINEEDGE API HELPER v2.2
# Production Broker Adapter
# Engine Compatible
# WebSocket Hardened
# REST Tick LTP Fallback Added
# ============================================================

from integrate import ConnectToIntegrate, IntegrateOrders, IntegrateData, IntegrateWebSocket
from logging import INFO, basicConfig, info
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

logger = logging.getLogger(__name__)
basicConfig(level=INFO)
pd.set_option('display.max_rows', None)


dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)
totp_secret = os.getenv("EDGE_TOTP_SECRET")

# ============================================================
# ORDER OBJECT
# ============================================================

class Order:
    def __init__(self, security_id, exchange_segment, transaction_type,
                 quantity, order_type, product_type, price: float = 0.0):
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

        self.api_session_key = None
        self.ws_session_key = None

        self.c2i = None
        self.ic = None
        self.io = None
        self.iws = None

        # WS STATE
        self._ws_running = False
        self._ws_logged_in = False

        # DATA BUFFERS
        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._order_buffer = {}
        self._order_lock = threading.Lock()

        # SUBSCRIPTION STATE (DEFERRED MODEL)
        self._subscribed = set()

        self._login()
        self._integrateData(self.c2i)
        self._integrateOrders(self.c2i)
        self._iWebsocket(self.c2i)

    # ========================================================
    # LOGIN
    # ========================================================

    def _login(self):

        c2i = ConnectToIntegrate()
        Totp = pyotp.TOTP(totp_secret).now()

        c2i.login(
            api_token=self.api_token,
            api_secret=self.api_secret,
            totp=Totp,
        )

        self.c2i = c2i
        self.api_session_key = c2i.api_session_key
        self.ws_session_key = c2i.ws_session_key

    def _integrateData(self, c2i):
        self.ic = IntegrateData(c2i)

    def _integrateOrders(self, c2i):
        self.io = IntegrateOrders(c2i)

    def _iWebsocket(self, c2i):

        iws = IntegrateWebSocket(c2i)

        #  WebSocket - CALLBACKS

        iws.on_login = self._on_ws_login
        iws.on_tick_update = self._on_tick_update
        iws.on_order_update = self._on_order_update
        iws.on_exception = self._on_ws_error 
        iws.on_close = self._on_ws_close
        
        self.iws = iws

    # ========================================================
    # WS CALLBACKS (CORRECTED)
    # ========================================================

    def _on_ws_login(self, iws):

        logger.info("WS LOGIN SUCCESS")

        self._ws_logged_in = True
        self._ws_running = True

        #  CRITICAL: SUBSCRIBE ONLY HERE
        if self._subscribed:
            try:
                iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_TICK,list(self._subscribed))
                
            except Exception as e:
                logger.error(f"Subscription error: {e}")

        # Order stream (safe here)
        try:
            iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_ORDER)
        except Exception as e:
            logger.error(f"Order stream error: {e}")

    def _on_ws_close(self, iws, code, reason):
        logger.warning(f"WS CLOSED: {code} {reason}")
        self._ws_running = False
        self._ws_logged_in = False

        iws.stop()

    def _on_ws_error(self, iws, e):
        logger.error(f"WS ERROR: {e}")

        iws.close_on_exception("Closing connection due to exception")        

    # ========================================================
    # TICK HANDLER (UNCHANGED)
    # ========================================================

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

            price = float(price)
            tick["lp"] = price

            with self._tick_lock:
                self._tick_cache[key] = tick

        except Exception as e:
            logger.error(f"Tick error: {e}")

    # ========================================================
    # ORDER HANDLER (UNCHANGED)
    # ========================================================

    def _on_order_update(self, iws, order):

        try:
            order_id = order.get("orderId")
            status = order.get("status")

            if order_id and status:
                with self._order_lock:
                    self._order_buffer[str(order_id)] = str(status).upper()

        except Exception as e:
            logger.error(f"Order error: {e}")

    # ========================================================
    # WS CONTROL (FIXED)
    # ========================================================

    def Start_Websocket(self):

        if self._ws_running:
            return

        try:
            self._ws_logged_in = False
            self.iws.connect(daemonize=True)

        except Exception as e:
            logger.error(f"WS start error: {e}")

    def Subscribe_inst(self, tokens):

        if not tokens:
            return

        # STORE ALWAYS
        for t in tokens:
            self._subscribed.add(t)

        #  ONLY SUBSCRIBE IF WS READY
        if self._ws_logged_in:
            try:
                self.iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_TICK,tokens)

            except Exception as e:
                logger.error(f"Subscribe error: {e}")

    def Unsubscribe_inst(self, tokens):

        if not tokens:
            return

        for t in tokens:
            self._subscribed.discard(t)

        if self._ws_logged_in:
            try:
                self.iws.unsubscribe(self.c2i.SUBSCRIPTION_TYPE_TICK,tokens
                                     )
            except Exception as e:
                logger.error(f"Unsubscribe error: {e}")

    def Close_Websocket(self):

        try:
            self._ws_running = False
            self._ws_logged_in = False
            self.iws.stop()

        except Exception as e:
            logger.error(f"WS close error: {e}")

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
        elif timeframe == "tick":
            tf = self.c2i.TIMEFRAME_TYPE_TICK
        else:
            tf = self.c2i.TIMEFRAME_TYPE_MIN

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
    # MASTER FILE (UNCHANGED)
    # ========================================================

    def download_master_zip(self, url="https://app.definedgesecurities.com/public/allmaster.zip"):

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
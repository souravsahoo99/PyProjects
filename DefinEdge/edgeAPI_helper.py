# ============================================================
# DEFINEEDGE API HELPER v3.1        
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
import zipfile
import requests
import pandas as pd
from typing import Union, Any
from datetime import datetime ,timedelta
from dotenv import find_dotenv, load_dotenv ,set_key

logger = logging.getLogger(__name__)
basicConfig(level=INFO)
pd.set_option('display.max_rows', None)


dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)
totp_secret = os.getenv("EDGE_TOTP_SECRET")

# ============================================================
#   DEFINEEDGE API CLASS
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

        # WEBSOCKET STATE
        self._ws_logged_in = False        
        self._ws_running = False

        # DATA BUFFERS
        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._order_buffer = {}
        self._order_lock = threading.Lock()

        # SUBSCRIPTION STATE (DefineEdge MODEL)
        self._subscribed: set[tuple[str,str]] = set()        # [(iws.c2i.EXCHANGE_TYPE_NSE, "11536"),(iws.c2i.EXCHANGE_TYPE_NSE, "3456"),]
                                                         # [("NSE", "26000"),("NSE", "26009"),("NFO", "66022"),("NFO", "66023")]
        self._login()
        self._integrateData(self.c2i)
        self._integrateOrders(self.c2i)
        self._iWebsocket(self.c2i)


    # ========================================================
    #    LOGIN                                                   
    # ========================================================

    def _login(self):

        c2i = ConnectToIntegrate()

        t_otp = pyotp.TOTP(totp_secret).now()

        c2i.login(api_token= self.api_token, api_secret= self.api_secret, totp= t_otp,)

        self.c2i = c2i
        self.api_session_key = c2i.api_session_key
        self.ws_session_key = c2i.ws_session_key

    def _integrateData(self, c2i):
        self.ic = IntegrateData(c2i)

    def _integrateOrders(self, c2i):
        self.io = IntegrateOrders(c2i)

    def _iWebsocket(self, c2i):

        iws = IntegrateWebSocket(c2i)

        # Assigning WebSocket CallBack Functions

        iws.on_login = self._on_ws_login
        iws.on_tick_update = self._on_tick_update
        iws.on_order_update = self._on_order_update
        iws.on_exception = self._on_ws_error 
        iws.on_close = self._on_ws_close
        
        self.iws = iws

    # ========================================================
    #   WebSocket CALLBACK FUNCTIONs    (FIXED)
    # ========================================================

    def _on_ws_login(self, iws):

        logger.info("WS LOGIN SUCCESS")
        self._ws_logged_in = True

        #  CRITICAL: SUBSCRIBE ONLY HERE
        if not self._subscribed:
            return
            
        tokens = list(self._subscribed)

        iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_TICK, tokens)
        iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_ORDER, tokens)       



    def _on_ws_error(self, iws, e):
        logger.error(f"WS Exception Error: {e}")

        iws.close_on_exception("Closing connection due to exception")       


    def _on_ws_close(self, iws, code, reason):
        logger.warning(f"WS CLOSED: {code} {reason}")
        self._ws_running = False
        self._ws_logged_in = False

        iws.stop()


    # =============   TICK HANDLER   =============

    def _on_tick_update(self, iws, tick):

        exchange = tick.get("e") 
        token = tick.get("tk")

        if not exchange or not token :
                return

        key = f"{exchange}|{token}"                     # Gagteway 'KEY' for fetching data
        price = tick.get("lp") or tick.get("ltp")

        if price is None:
            return

        tick["lp"] = float(price)

        with self._tick_lock:
            self._tick_cache[key] = tick

    # ===========   ORDER HANDLER   ==============

    def _on_order_update(self, iws, order):

        order_id = order.get("order_id")
        status = order.get("status")

        if order_id and status:
            with self._order_lock:
                self._order_buffer[str(order_id)] = str(status).upper()


    # ========================================================
    #     WebSocket  CONTROLs  for  Api-Engine
    # ========================================================

    def Start_Websocket(self):

        self.iws.connect(daemonize=True)
        # iws.connect(daemonize=True, ssl_verify=False)  <  Replace this if above line isn't working #

        self._ws_running = True 
              

    def Close_Websocket(self):

        try:
            self._ws_running = False
            self._ws_logged_in = False
            self.iws.stop()

        except Exception as e:
            logger.error(f"WS close error: {e}")

    # ========    Additional Websocket Functions    ========

    def Subscribe_inst(self,exchange, token):

        t = (exchange,str(token))
        if t not in self._subscribed:    
            self._subscribed.add(t)

        # # ONLY SUBSCRIBE IF WS READY (Not Applicable for DefineEdge_SDK)
    """ if self._ws_logged_in:
            try:
                self.iws.subscribe(self.c2i.SUBSCRIPTION_TYPE_TICK,t)
            except Exception as e:
                logger.error(f"Subscribe error: {e}") """

    def Unsubscribe_inst(self,exchange, token):

        t = (exchange,str(token))

        if t in self._subscribed:
            self._subscribed.discard(t)

        # # ONLY UNSUBSCRIBE IF WS READY (Not Applicable for DefineEdge_SDK)
    """ if self._ws_logged_in:
            try:
                self.iws.unsubscribe(self.c2i.SUBSCRIPTION_TYPE_TICK,t)                                     
            except Exception as e:
                logger.error(f"Unsubscribe error: {e}") """


    # ========================================================
    #     IntegrateOrders  METHODS
    # ========================================================

    def Place_Order(self,exchange: str,order_type: str,price: float,price_type: str,product_type: str,quantity: int,tradingsymbol: str,):
        
        order = self.io.place_order( 
            exchange=exchange,
            order_type=order_type,
            price=price,
            price_type=price_type,
            product_type=product_type,
            quantity=quantity,
            tradingsymbol=tradingsymbol,
        )
        return order


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

    def Get_Order_By_ID(self, order_id):

        return self.io.order(order_id)

    # ========================================================                          
    def Get_Positions(self):

        return self.io.positions()

    def Get_Orderbook(self):

        return self.io.orders()

    def Get_TradeBook(self):

        return self.io.trades()

    # ========================================================
    #     IntegrateData  (REST)  OHLC fetching
    # ========================================================

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
    #     REST TICK DATA (LTP FALLBACK)
    # ========================================================

    def Get_LTP(self, exchange, trading_symbol):
        
        end_   = datetime.now()
        start_ = end_ - timedelta(minutes=1)

        tick = self.ic.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=self.c2i.TIMEFRAME_TYPE_TICK,
            start=start_,
            end=end_
            )        
        
        df = pd.DataFrame(list(tick))
        ltp:float = df.iloc[-1]["ltp"]       
        
        return ltp      

    # ________________________________________________________
    # ============  Token Fetching from Symbols  =============

    def get_token_for_symbol(self,exchange: str, symbol: str) -> tuple[str, str]:

        if exchange not in self.c2i.exchange_types:
            raise ValueError("Invalid exchange type")

        token: Union[str, None] = next(
            (
                i["token"]
                for i in self.c2i.symbols
                if i["segment"] == exchange and i["trading_symbol"] == symbol
            ),
            None,
        )
        if token:
            return (exchange, token)
        else:
            raise Exception(f"Token not found for {symbol} in symbols file")


# -----------------------------------------------------------
# ================= Download MASTER File ====================
# ___________________________________________________________

def Load_Master(url="https://app.definedgesecurities.com/public/allmaster.zip"):

    response = requests.get(url)

    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:

        name = z.namelist()[0]

        with z.open(name) as f:

            df = pd.read_csv(f, header=None, low_memory=False)

    return df




#_#_#_#_#_#
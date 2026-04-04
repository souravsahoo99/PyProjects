# ============================================================
# DEFINEEDGE API HELPER v1.1        
# Production Broker Adapter
# Engine Compatible
# WebSocket Hardened
# REST Tick LTP Fallback Added
# ============================================================

from dhanhq import dhanhq, DhanContext, MarketFeed, FullDepth, DhanLogin,ForeverOrder
from logging import INFO, basicConfig, info
import os
import io
import json
import logging
import threading
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


# ============================================================
#   DHAN HQ API CLASS
# ============================================================

class DhanApi:

    def __init__(self, client_id, access_token):
        self.client_id = str(client_id)
        self.access_token = str(access_token)
        
        #dhan_context==
        self.context = DhanContext(self.client_id, self.access_token)
        # Context Initialize
        self.api = dhanhq(self.context)
        #_____________________________________________________
        # WEBSOCKET STATE              _______________________
        self.market_feed = None        # Web _ Socket

        self._ws_logged_in = False        
        self._ws_running = False

        # DATA BUFFERS
        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        # SUBSCRIPTION Instruments
        self._subscribed = []        # [(MarketFeed.NSE, "11536", MarketFeed.Ticker),(MarketFeed.NSE, "1333", MarketFeed.Quote)]
                                         ## Format: [(ExchangeSegment, SecurityID, InstrumentType)]               
        self._login()

    # ========================================================
    #    LOGIN                                                   
    # ========================================================

    def _login(self):

        pass

    # =============   TICK HANDLER   =============

    def _on_tick_update(self, data):

        exchange = data.get("e") 
        token = data.get("tk")

        if not exchange or not token :
                return

        key = f"{exchange}|{token}"                     # Gagteway 'KEY' for fetching data
        price = data.get("lp") or data.get("ltp")

        if price is None:
            return

        tick = float(price)

        with self._tick_lock:
            self._tick_cache[key] = tick

    # ========================================================
    #     WebSocket  CONTROLs  for  Api-Engine
    # ========================================================

    def initialise_Websocket(self):
        version = "v2"

        market_feed = MarketFeed(self.context, self._subscribed, version)
        self._ws_logged_in = True  

        return market_feed


    def Start_Websocket(self):

        ws = self.initialise_Websocket()
        self.market_feed = ws

        try:
            print("Connecting to Market Feed...")
            ws.run_forever()
            self._ws_running = True 
        
        except Exception as e:
            print(f"WS Connect Error: {e}")


        while self._ws_running == True :
            
            data = ws.get_data()
            try:
                self._on_tick_update(data)

            except Exception as e:
                print(f"Error: {e}")


    def Close_Websocket(self):

        try:
            self.market_feed.disconnect
            self._ws_running = False

        except Exception as e:
            logger.error(f"WS close error: {e}")

    # ======== Utilities ==============
    def _resolute_ws_exchange(self,exchange):        
        # Constants for Exchange Segment(MarketFeed)
        """ 
        IDX = 0
        NSE = 1
        NSE_FNO = 2
        NSE_CURR = 3
        BSE = 4
        MCX = 5
        BSE_CURR = 7
        BSE_FNO = 8
        """
    
        if exchange in ["IDX", "Idx", "idx"]:
            return MarketFeed.IDX
        
        elif exchange in ["NSE", "Nse", "nse"]:
            return MarketFeed.NSE
        elif exchange in ["NFO", "Nfo", "nfo"]:
            return MarketFeed.NSE_FNO

        elif exchange in ["BSE", "Bse", "bse"]:
            return MarketFeed.BSE
        elif exchange in ["BFO", "Bfo", "bfo"]:
            return MarketFeed.BSE_FNO
        
        elif exchange in ["MCX", "Mcx", "mcx"]:
            return MarketFeed.MCX      

        elif exchange in ["CUR","Cur","CURR","Curr"]:
            return MarketFeed.NSE_CURR
        else:
            raise Exception (f"{exchange} is not valid. Use NSE, BSE, NFO, BFO, MCX, IDX, CURR.")    
        
    def _resolute_exchange(self,exchange):        
        # Constants for Exchange Segment(MarketFeed)
        """Constants for Exchange Segment"""
        NSE = 'NSE_EQ'
        BSE = 'BSE_EQ'
        CUR = 'NSE_CURRENCY'
        MCX = 'MCX_COMM'
        FNO = 'NSE_FNO'
        NSE_FNO = 'NSE_FNO'
        BSE_FNO = 'BSE_FNO'
        INDEX = 'IDX_I'

        """Constants for Transaction Type"""
        BUY = 'BUY'
        SELL = 'SELL'

        """Constants for Product Type"""
        CNC = 'CNC'
        INTRA = "INTRADAY"
        MARGIN = 'MARGIN'
        CO = 'CO'
        BO = 'BO'
        MTF = 'MTF'

        """Constants for Order Type"""
        LIMIT = 'LIMIT'
        MARKET = 'MARKET'
        SL = "STOP_LOSS"
        SLM = "STOP_LOSS_MARKET"

        """Constants for Validity"""
        DAY = 'DAY'
        IOC = 'IOC'


        if   exchange in ["IDX", "Idx", "idx"]:
            return self.api.INDEX
        
        elif exchange in ["NSE", "Nse", "nse"]:
            return self.api.NSE
        elif exchange in ["NFO", "Nfo", "nfo"]:
            return self.api.NSE_FNO

        elif exchange in ["BSE", "Bse", "bse"]:
            return self.api.BSE
        elif exchange in ["BFO", "Bfo", "bfo"]:
            return self.api.BSE_FNO
        
        elif exchange in ["MCX", "Mcx", "mcx"]:
            return self.api.MCX      

        elif exchange in ["CUR","Cur","CURR","Curr"]:
            return self.api.CUR
        else:
            raise Exception (f"{exchange} is not valid. Use NSE, BSE, NFO, BFO, MCX, IDX, CUR.")

    # ========    Additional Websocket Functions    ========

    def Subscribe_inst(self,exchange, token):

        exch = self._resolute_ws_exchange(exchange)

        t = (exch,str(token), MarketFeed.Ticker)

        if t not in self._subscribed:    
            self._subscribed.add(t)


    def Unsubscribe_inst(self,exchange, token):

        exch = self._resolute_ws_exchange(exchange)

        t = (exch,str(token), MarketFeed.Ticker)

        if t in self._subscribed:
            self._subscribed.discard(t)


    # ========================================================
    #     IntegrateData  (REST)  OHLC fetching
    # ========================================================

    def Get_Intraday_Data(self,token,exchange,inst_type,start,end):
        exch_ = self._resolute_exchange(exchange)
        try:
            intraday_data = self.api.intraday_minute_data(
                security_id=str(token),
                exchange_segment=exch_,
                instrument_type=str(inst_type),
                from_date=str(start),
                to_date=str(end)
            )
            print(intraday_data)
        except Exception as e:
            print(f"Error fetching intraday data: {e}")


    def Get_Daily_Data(self,token,exchange,inst_type,start,end):
        exch_ = self._resolute_exchange(exchange)
        try:
            daily_data = self.api.historical_daily_data(
                security_id=str(token),
                exchange_segment=exch_,
                instrument_type=str(inst_type),
                from_date=str(start),
                to_date=str(end)
            )
            print(daily_data)
        except Exception as e:
            print(f"Error fetching daily data: {e}")


    # =============
    #     REST TICK DATA (LTP FALLBACK)
    # =============

    def Get_Tick_Data(self,exchange,trading_symbol,start,end):


        ticks = self.ic.historical_data(
            exchange=exchange,
            trading_symbol=trading_symbol,
            timeframe=self.c2i.TIMEFRAME_TYPE_TICK,
            start=start,
            end=end
            )

        return ticks
    
    # ====================================================

    def Get_LTP(self, exchange, trading_symbol):
        
        end_   = datetime.now()
        start_ = end_ - timedelta(minutes=1)

        tick = None
   




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
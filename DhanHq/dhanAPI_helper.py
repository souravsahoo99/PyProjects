# ============================================================
# DEFINEEDGE API HELPER v1.0        
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
import time
import logging
import traceback
import threading
import requests
import urllib.parse
import pandas as pd
from typing import Union, Any
from datetime import datetime , timedelta
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

    def __init__(self, ClientCode: str, api_key: str , api_secret:str ):

        self.ClientCode = ClientCode
        self.api_key = api_key
        self.api_secret = api_secret
        
        # dhan_context==
        self.dhan_context = None
        # Context Initialize
        self.api = None
        
        # Instrument Data Frame
        self.instrument_df = None 
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
        self.login_okay = self._login()

# =================================================================================================================================================================== 
		
    def _extract_token_id_from_input(self, text: str) -> str:
        try:
            parsed = urllib.parse.urlparse(text)
            qs = urllib.parse.parse_qs(parsed.query)
            if "tokenId" in qs and qs["tokenId"]:
                return qs["tokenId"][0]
        except Exception:
            pass            
        return "Token ID not Valid"

    def extract_access_token(self, resp: dict) -> str:
        if not isinstance(resp, dict):
            raise Exception(f"Invalid response type: {type(resp)} => {resp}")
        token = resp.get("accessToken") or resp.get("access_token")
        if token:
            return token
        data = resp.get("data")
        if isinstance(data, dict):
            token = data.get("accessToken") or data.get("access_token")
            if token:
                return token
            raise Exception(f"Access token not found in response: {resp}")
	
    def _token_file_today(self) -> str:
        os.makedirs("Dependencies", exist_ok=True)
        today = datetime.date.today().strftime("%Y-%m-%d")
        return os.path.join("Dependencies", f"token_{today}.txt")

    def _delete_all_token_files(self):
        os.makedirs("Dependencies", exist_ok=True)
        for name in os.listdir("Dependencies"):
            if name.startswith("token_") and name.endswith(".txt"):
                try:	
                    os.remove(os.path.join("Dependencies", name))
                except:
                    pass

    def _read_token_today(self) -> str:
        p = self._token_file_today()
        if not os.path.exists(p):
            return ""
        raw = open(p, "r", encoding="utf-8").read().strip()
        return raw.split("|", 1)[-1].strip()
	
    def _save_token_today_once(self, token: str):
        p = self._token_file_today()
        if os.path.exists(p):
            return
        self._delete_all_token_files()
        today = datetime.date.today().strftime("%Y-%m-%d")
        open(p, "w", encoding="utf-8").write(f"{today}|{token}")

    def _token_path_today(self) -> str:
        date_str = str(datetime.datetime.now().date())
        os.makedirs("Dependencies", exist_ok=True)
        return f"Dependencies/token_{self.ClientCode}_{date_str}.txt"
    
    # ========================================================
    #    LOGIN                                                   
    # ========================================================

    def _login(self) -> bool:

        try:
            print("Attempting authentication using API-KEY ")
            
            saved = self._read_token_today()

            if saved:
                self.token_id = saved
                self.dhan_context = DhanContext(self.ClientCode, self.token_id)
                self.Dhan = dhanhq(self.dhan_context)
                self.instrument_df = self.get_instrument_file()
                print("Already logged in for today, so reusing the token")
                return True

            login = DhanLogin(self.ClientCode)

            login.generate_login_session(self.api_key, self.api_secret)
            print("\nPaste the redirect URL after login")
            print("Example: https://www.google.com/?tokenId=xxxx-xxxx-xxxx\n")

            while True :
                user_input = input("Paste redirect URL ---").strip()
                if not user_input:
                    print("Empty input. Paste again.")
                    continue  

                extracted_token_id = self._extract_token_id_from_input(user_input)
                if not extracted_token_id:
                    print("Could not find tokenId in what you pasted. Try again.")
                    continue

                res = login.consume_token_id(extracted_token_id, self.api_key, self.api_secret)
                access_token = self.extract_access_token(res)
                self.token_id = access_token
                self._save_token_today_once(self.token_id)
                self.dhan_context = DhanContext(self.ClientCode, self.token_id)
                self.api = dhanhq(self.dhan_context) #################################################
                 
                self.instrument_df = self.get_instrument_file()
                print("Instrument file retrieved successfully")
                return True                

        except Exception as e:
            print("Login Failed: {e}")
        
            self.logger.exception(f"Login failed: {e}")
            traceback.print_exc()
            return False



# =================================================================================================================================================================== 
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

# -----------------------------------------------------------
# ================= Download MASTER File ====================
# ___________________________________________________________
    def get_instrument_file(self):

        global instrument_df 
        current_date = time.strftime("%Y-%m-%d")
        expected_file = 'all_instrument ' + str(current_date) + '.csv'
        for item in os.listdir("Dependencies"):
            path = os.path.join(item)

            if (item.startswith('all_instrument')) and (current_date not in item.split(" ")[1]):
                if os.path.isfile("Dependencies\\" + path):
                    os.remove("Dependencies\\" + path)

        if expected_file in os.listdir("Dependencies"):
            try:
                print(f"reading existing file {expected_file}")
                instrument_df = pd.read_csv("Dependencies\\" + expected_file, low_memory=False)         
                
            except Exception as e:
                print("This BOT Is Instrument file is not generated completely, Picking New File from Dhan Again")
                instrument_df = pd.read_csv("https://images.dhan.co/api-data/api-scrip-master.csv", low_memory=False)
                instrument_df['SEM_CUSTOM_SYMBOL'] = instrument_df['SEM_CUSTOM_SYMBOL'].str.strip().str.replace(r'\s+', ' ', regex=True)
                instrument_df.to_csv("Dependencies\\" + expected_file)
        else:
			# this will fetch instrument_df file from Dhan
            print("System is fetching the latest instrument file from Dhan")
            instrument_df = pd.read_csv("https://images.dhan.co/api-data/api-scrip-master.csv", low_memory=False)
            instrument_df['SEM_CUSTOM_SYMBOL'] = instrument_df['SEM_CUSTOM_SYMBOL'].str.strip().str.replace(r'\s+', ' ', regex=True)
            instrument_df.to_csv("Dependencies\\" + expected_file)
        
        return instrument_df





#_#_#_#_#_#
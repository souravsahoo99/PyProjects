# ============================================================
# TOKEN REGISTRY v12.7
# Official DefineEdge Replica Engine
# Backward Compatible | Thread Safe
# ============================================================

import os
import io
import zipfile
import requests
import threading
import time as t_
from csv import reader
from typing import Union
from datetime import datetime, time, timedelta
from os.path import abspath, dirname, join

# TokenMap for Global Scope               

TOKEN_BUS = []               

# ============================================================
#   Exchange CLOCK   
# ============================================================

class Exchange_Clock:
    
    def __init__(self, exchange=None):
        self.exch  = str(exchange)
        self._clock = None

        self._exchange_clock()
    
    def _exchange_clock(self):
        if self.exch in ["CDS" , "Cds" , "cds"]:
            self._clock = time(9, 0)
        elif self.exch in ["MCX", "Mcx", "mcx"]:
            self._clock = time(9, 0)
        else:
            self._clock = time(9, 15)

    def clock(self):
        return self._clock

    def is_open(self):
        open = self._clock
        close = None

        if self.exch in ["CDS" , "Cds" , "cds"]:
            close= time(17, 0)  
        elif self.exch in ["MCX", "Mcx", "mcx"]:
            close = time(23, 30)      
        else:
            close = time(15, 30)

        now = datetime.now().time()

        if now < open :
            print (f"Exchange {self.exch} isn't Opened")  
            return False  
        elif now >= open and now < close:
            print (f"Exchange {self.exch} is Open")     ## Running Market  
            return True        
        elif now >= close:
            print (f"Exchange {self.exch} is Closed")
            return False
        else:
            raise Exception ("[Exchange_Clock] Unknown Error !!!")

# ============================================================
#   TOKEN REGISTRY   
# ============================================================

class TokenRegistry:

    MASTER_URL = "https://app.definedgesecurities.com/public/allmaster.zip"

    def __init__(self, api=None):

        self.api = api

        # LIVE CACHE DATA
        self.symbol_to_token = {}      # (exchange, symbol) → token
        self.token_to_symbol = {}      # (exchange, token) → symbol
        self.symbol_token_map = {}     # symbol → token

        self._loaded = False
        self._lock = threading.Lock()        

        # INTERNAL STORAGE PATH
        self._symbols_file = abspath(join(dirname(__file__), "allmaster.csv"))

# ============================================================
#    DOWNLOAD - MASTER ZIP & EXTRACT CSV
# ============================================================

    def _ensure_master_file(self):

        # ---------- FILE NOT PRESENT ----------
        if not os.path.exists(self._symbols_file):

            response = requests.get(self.MASTER_URL, timeout=10)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content), "r") as z:
                z.extract("allmaster.csv", abspath(dirname(__file__)))

            return False   # no reload needed (at first load)

        # ---------- FILE EXISTS → CHECK FRESHNESS ----------
        file_timestamp = os.path.getmtime(self._symbols_file)
        file_date = datetime.fromtimestamp(file_timestamp).date()

        today_ = datetime.now().date()
        current_time = datetime.now().time()
        EDGE_upload_time = time(8, 0)

        # ----------  RE-DOWNLOAD CONDITIONs  ----------
        if file_date < today_ and current_time >= EDGE_upload_time:

            print("[TOKEN_REGISTRY] Refreshing master CSV...")

            response = requests.get(self.MASTER_URL, timeout=10)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content), "r") as z:
                z.extract("allmaster.csv", abspath(dirname(__file__)))
          
            t_.sleep(0.1)
            return True    # trigger reload in 'symbol_generator'

        # ----------  OTHERWISE USE EXISTING  ----------
        else: 
            return False

# ============================================================
#    GENERATOR 
# ============================================================
    def _symbol_generator_raw(self):

        with open(self._symbols_file, "r") as fp:
            
            csv_reader = reader(fp)

            for line in csv_reader:
                yield {
                    "segment": line[0],
                    "token": line[1],
                    "symbol": line[2],
                    "trading_symbol": line[3],
                    "instrument_type": line[4],
                    "expiry": line[5],
                    "tick_size": line[6],
                    "lot_size": line[7],
                    "option_type": line[8],
                    "strike": str(int(int(line[9]) /(int(line[11]) * (10 ** int(line[10]))))),
                    "isin": line[12],
                    "price_mult": line[13],
                }

# ============================================================
# GENERATOR WITH RELOAD
# ============================================================

    def _symbol_generator(self):

        refresh_trigger = self._ensure_master_file()

        if refresh_trigger and self._loaded:
            self._reload_master_atomic()

        for line in self._symbol_generator_raw():
            yield line

# ============================================================
# ATOMIC RELOAD (CORE)
# ============================================================

    def _reload_master_atomic(self):

        print("[TOKEN_REGISTRY] Rebuilding cache (atomic)...")

        new_symbol_to_token = {}
        new_token_to_symbol = {}
        new_symbol_token_map = {}

        for item in self._symbol_generator_raw():

            try:
                exchange = item["segment"]
                token = str(item["token"])
                symbol = item["trading_symbol"]

                if not exchange or not symbol or not token:
                    continue

                symbol = symbol.upper().strip()

                new_symbol_to_token[(exchange, symbol)] = token
                new_token_to_symbol[(exchange, token)] = symbol
                new_symbol_token_map[symbol] = token

            except Exception:
                continue

        # WITH Thread.Lock ATOMIC SWAP
        with self._lock:
            self.symbol_to_token = new_symbol_to_token
            self.token_to_symbol = new_token_to_symbol
            self.symbol_token_map = new_symbol_token_map
            
            self._loaded = True

        print("[TOKEN_REGISTRY] Cache swap complete (zero downtime)")

# ============================================================
# INITIAL LOAD
# ============================================================

    def load_master(self):

        if self._loaded:
            return

        self._reload_master_atomic()


# ===============================================================================
#              OFFICIAL BROKER LOOKUP METHOD   (Preserved for Backup)           |
#________________________________________________________________________________       
    def get_token_for_symbol(self, exchange: str, symbol: str) -> tuple[str, str]:

        token: Union[str, None] = next(
            (
                i["token"]
                for i in self._symbol_generator()
                if i["segment"] == exchange and i["trading_symbol"] == symbol
            ),
            None,
        )

        if token:
            return (exchange, token)

        raise Exception(f"Token not found for {symbol}")


# ============================================================
#    CORE  LOOKUPS
# ============================================================

    def get_symbol(self, exchange, token):
        return self.token_to_symbol.get((exchange, str(token)))


    def reg_inst(self,symbol_type,token):

        entry = {"symbol_type": symbol_type,"token": token,"key": f"{symbol_type}|{token}"}

        if entry not in TOKEN_BUS:        
            TOKEN_BUS.append(entry)

    # ========== Token Fetching from Cache Buffer ============
    def get_token(self, exchange, trading_sym):

        if not self._loaded:
            self.load_master()

        with self._lock:
            TOKEN = self.symbol_to_token.get((exchange, trading_sym))     # No Upper Casing needed

        if not TOKEN:
            raise Exception("TOKEN not found ")

        derived_symbol = self.get_symbol(exchange, TOKEN)
        self.reg_inst(symbol_type=derived_symbol ,token=TOKEN)

        return TOKEN

# ============================================================
#   EXPIRY GENERATOR (PATH B EXTENSION)
# ============================================================

    def get_nearest_expiry(self,exchange: str, symbol: str, inst_type:str):

        exc_ = None
        if exchange in ["NFO", "MCX", "BFO", "CDS"]:
            exc_ = exchange
        else:
            raise Exception ("'EXCHANGE_TYPE' Mismatch")            

        inst_ = None
        if inst_type in ["OPTIDX", "OPTSTK", "OPTFUT", "FUTIDX", "FUTSTK", "FUTCOM", "OPTCUR", "FUTCUR"]:
            inst_ = inst_type
        else:
            raise Exception ("'INSTRUMENT_TYPE' Mismatch")
        

        expiries = set()
        today = datetime.now().date()

        for i in self._symbol_generator():

            if (i["segment"] == exc_ and i["symbol"] == symbol and i["instrument_type"] == inst_):
                expiries.add(i["expiry"])

        if not expiries:
            raise Exception("No Expiry Found")

        expiry_dates = sorted([
            datetime.strptime(exp, "%d%m%Y").date()
            for exp in expiries
        ])

        for exp_date in expiry_dates:
            if exp_date > today:
                return exp_date.strftime("%d%m%Y")

        raise Exception(f"No valid expiry found for {symbol}")


# ============================================================
#   OPTION SYMBOL GENERATOR (PATH B CORE)
# ============================================================

    def get_symbol_for_option(self,exchange: str, symbol: str, inst_type:str, strike:str, opt_type:str , expiry:str):

        exc_ = None
        if exchange in ["NFO", "MCX", "BFO", "CDS"]:
            exc_ = exchange
        else:
            raise Exception ("'EXCHANGE_TYPE' Mismatch")          

        inst_ = None
        if inst_type in ["OPTSTK", "OPTIDX"]:
            inst_ = inst_type
        elif inst_type == "OPTFUT" :
            inst_ = inst_type
            exc_ = "MCX"
        elif inst_type == "OPTCUR" :
            inst_ = inst_type
            exc_ = "CDS"            
        else:
            raise Exception ("'INSTRUMENT_TYPE' Mismatch")


        strike_symbol: Union[str, None] = next(
            (
                i["trading_symbol"]
                for i in self._symbol_generator()
                if i["segment"] == exc_ and i["symbol"] == symbol and i["instrument_type"] == inst_ and i["expiry"] == expiry and i["option_type"] == opt_type  and i["strike"] == strike
            ),
            None,
        )
                
        if strike_symbol:
            return (strike_symbol)

        else:
            raise Exception(f"SYMBOL not found for {symbol} in MASTER file")

# ============================================================
#   INDEX SYMBOL GENERATOR
# ============================================================

    def get_symbol_for_Index(self,exchange: str, symbol: str, inst_type:str , opt_type:str  ):

        exc_ = None
        if exchange in ["NFO", "BFO", "CDS", "MCX"]:
            raise Exception ("'EXCHANGE_TYPE' Mismatch")             
        else: exc_ = exchange  

        inst_ = None
        if inst_type in ["IDX", "EQ"]:
            inst_= inst_type

        strike_symbol: Union[str, None] = next(
            (
                i["trading_symbol"]
                for i in self._symbol_generator()
                if i["segment"] == exc_ and i["symbol"] == symbol and i["instrument_type"] == inst_  and i["option_type"] == opt_type 
            ),
            None,
        )
                
        if strike_symbol:
            return (strike_symbol)
        else:
            raise Exception(f"SYMBOL not found for {symbol} in MASTER file")        

# ============================================================
#   FUTURES SYMBOL GENERATOR
# ============================================================

    def get_symbol_for_futures(self,exchange: str, symbol: str, inst_type:str , expiry:str ):

        exc_ = None
        if exchange in ["NFO", "BFO", "MCX", "CDS"]:
            exc_ = exchange
        else:
            raise Exception ("'EXCHANGE_TYPE' Mismatch")            

        inst_ = None
        if inst_type in ["FUTSTK" , "FUTIDX"]:
            inst_ = inst_type
        elif inst_type == "FUTCOM" :
            inst_ = inst_type
            exc_ = "MCX"
        elif inst_type == "FUTCUR" :
            inst_ = inst_type
            exc_ = "CDS"            
        else:
            raise Exception ("'INSTRUMENT_TYPE' Mismatch")

        strike_symbol: Union[str, None] = next(
            (
                i["trading_symbol"]
                for i in self._symbol_generator()
                if i["segment"] == exc_ and i["symbol"] == symbol and i["instrument_type"] == inst_ and i["expiry"] == expiry 
            ),
            None,
        )
                
        if strike_symbol:
            return (strike_symbol)

        else:
            raise Exception(f"SYMBOL not found for {symbol} in MASTER file")            
          

# ============================================================
# ATM OPTIONS (MODIFIED)
# ============================================================

    def register_atm_options(self, engine, parent_symbol, parent_exchange, child_exchange, strike_dist):

        if not self._loaded:
            raise Exception("TokenRegistry not initialized")

        pt_symbol = self.get_symbol_for_Index(parent_exchange,parent_symbol)

        spot_price = engine.get_ltp_rest( exchange=parent_exchange, trade_sym=pt_symbol)

        if spot_price is None:
            raise Exception(f"Spot price unavailable for {parent_symbol}")

        strike = str(int(round(float(spot_price) / strike_dist) * strike_dist))

        option_symbol = None
        if parent_symbol == "Nifty 50" and parent_exchange == "NSE":
            # intentionally Hard-Coded for future compatibility
            option_symbol = "NIFTY"
        else:
            raise Exception("Need Opton_Symbol Value")
            

        expiry = self.get_nearest_expiry(exchange=child_exchange, symbol=option_symbol)

        ce_symbol = self.get_symbol_for_option(exchange=child_exchange, symbol=option_symbol, inst_type="OPTIDX", strike=strike, opt_type="CE" , expiry=expiry)
        pe_symbol = self.get_symbol_for_option(exchange=child_exchange, symbol=option_symbol, inst_type="OPTIDX", strike=strike, opt_type="PE", expiry=expiry)

        ce_token = self.get_token(child_exchange, ce_symbol)
        pe_token = self.get_token(child_exchange, pe_symbol)

        if ce_token and pe_token:
            return [(ce_symbol, ce_token),(pe_symbol, pe_token)]

        else:
            raise Exception ("Error: fetching CE/PE symbol & token")

# ============================================================
#   LEGACY PLACEHOLDERS (STRICT PROTOCOL)
# ============================================================

    def get_by_token(self, token):

        token = str(token)

        for (exchange, tk), symbol in self.token_to_symbol.items():
            if tk == token:
                return {"exchange": exchange,"symbol": symbol,"token": token}
            

    def get_strikes(self, symbol, expiry):
        # Placeholder
        return []






#_#_#_#_#_#_#_#
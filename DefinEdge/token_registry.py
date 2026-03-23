# ============================================================
# TOKEN REGISTRY v11.3
# Official DefineEdge Replica Engine
# Backward Compatible | Placeholder Safe
# ============================================================

import os
import io
import zipfile
import requests
from csv import reader
from typing import Union
from datetime import datetime, timedelta
from os.path import abspath, dirname, join

# TokenMap for Global Scope

TOKEN_BUS = []               

# ============================================================
#   TOKEN REGISTRY   
# ============================================================


class TokenRegistry:

    MASTER_URL = "https://app.definedgesecurities.com/public/allmaster.zip"

    def __init__(self, api=None):

        self.api = api

        # ----------------------------------------------------
        # CORE MAPS
        # ----------------------------------------------------
        self.symbol_to_token = {}      # (exchange, symbol) → token
        self.token_to_symbol = {}      # (exchange, token) → symbol
        self.symbol_token_map = {}     # symbol → token

        self._loaded = False

        self._registered_keys = set()   # (exchange, token)
        # ____________________________________________________
        # INTERNAL STORAGE
        # ----------------------------------------------------

        self._symbols_file = abspath(join(dirname(__file__), "allmaster.csv"))

# ============================================================
# DOWNLOAD
# ============================================================

    def _ensure_master_file(self):

        try:
            open(self._symbols_file, "r")
            return
        except FileNotFoundError:
            pass

        response = requests.get(self.MASTER_URL)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content), "r") as z:
            z.extract("allmaster.csv", abspath(dirname(__file__)))


# ============================================================
# GENERATOR
# ============================================================

    def _symbol_generator(self):

        self._ensure_master_file()

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
# OFFICIAL LOOKUP
# ============================================================

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
# LOAD MASTER
# ============================================================

    def load_master(self):

        for item in self._symbol_generator():

            try:
                exchange = item["segment"]
                token = str(item["token"])
                symbol = item["trading_symbol"]

                if not exchange or not symbol or not token:
                    continue

                symbol = symbol.upper().strip()

                self.symbol_to_token[(exchange, symbol)] = token
                self.token_to_symbol[(exchange, token)] = symbol
                self.symbol_token_map[symbol] = token

            except Exception:
                continue

        self._loaded = True


# ============================================================
# CORE LOOKUPS
# ============================================================

    def get_symbol(self, exchange, token):
        return self.token_to_symbol.get((exchange, str(token)))


    def reg_inst(self,symbol_type,token):

        entry = {"symbol_type": symbol_type,"token": token,"key": f"{symbol_type}|{token}"}

        if entry not in TOKEN_BUS:        
            TOKEN_BUS.append(entry)

    # ========== Token Fetching from Cache Buffer ============

    def get_token(self, exchange, trading_sym):


        if self._loaded == True:

            TOKEN = self.symbol_to_token.get((exchange, trading_sym.upper()))

            if not TOKEN:
                raise Exception("TOKEN not found ")

            trade_symbol = self.get_symbol(exchange, TOKEN)

            self.reg_inst(symbol_type=trade_symbol ,token=TOKEN)

            return TOKEN

        else:
            raise Exception("Registry isn't Loaded")


# ============================================================
# OPTION SYMBOL GENERATOR (PATH B CORE)
# ============================================================


    def get_symbol_for_option(self,exchange: str, symbol: str, inst_type:str, strike:str, opt_type:str , expiry:str):


        strike_symbol: Union[str, None] = next(
            (
                i["trading_symbol"]
                for i in self._symbol_generator()
                if i["segment"] == exchange and i["symbol"] == symbol and i["instrument_type"] == inst_type and i["expiry"] == expiry and i["option_type"] == opt_type  and i["strike"] == strike
            ),
            None,
        )
                
        if strike_symbol:
            return (strike_symbol)

        else:
            raise Exception(f"SYMBOL not found for {symbol} in MASTER file")

    def get_symbol_for_Index(self,exchange: str, symbol: str, inst_type:str = "IDX", opt_type:str = "IDX" ):

        strike_symbol: Union[str, None] = next(
            (
                i["trading_symbol"]
                for i in self._symbol_generator()
                if i["segment"] == exchange and i["symbol"] == symbol and i["instrument_type"] == inst_type  and i["option_type"] == opt_type 
            ),
            None,
        )
                
        if strike_symbol:
            return (strike_symbol)
        else:
            raise Exception(f"SYMBOL not found for {symbol} in MASTER file")        

# ============================================================
# EXPIRY GENERATOR (PATH B EXTENSION)
# ============================================================

    def get_nearest_expiry(self, exchange, symbol):

        today = datetime.now().date()

        expiries = set()

        for i in self._symbol_generator():

            if (i["segment"] == exchange and i["symbol"] == symbol and i["instrument_type"] == "OPTIDX"):
                expiries.add(i["expiry"])


        expiry_dates = sorted([
            datetime.strptime(exp, "%d%m%Y").date()
            for exp in expiries
        ])

        for exp_date in expiry_dates:
            if exp_date > today:
                return exp_date.strftime("%d%m%Y")

        raise Exception(f"No valid expiry found for {symbol}")


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
            pass
            

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
            

    def get_index_spot_token(self, symbol):
        return self.symbol_token_map.get(symbol.upper())


    def get_option_token(self, symbol, expiry, strike, option_type):
        # Placeholder (logic not defined)
        return None


    def get_strikes(self, symbol, expiry):
        # Placeholder
        return []


    def get_futures(self, symbol):
        # Placeholder
        return []





#_#_#_#_#
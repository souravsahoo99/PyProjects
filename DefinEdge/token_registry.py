# ============================================================
# ============================================================
# TOKEN REGISTRY v11.1
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
#   INTERNAL: DOWNLOAD + EXTRACT  (OFFICIAL REPLICA)
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
#   INTERNAL: SYMBOL GENERATOR (OFFICIAL REPLICA)
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


# ================ Official Calling Function ====================

    def get_token_for_symbol(self,exchange: str, symbol: str) -> tuple[str, str]:

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
        else:
            raise Exception(f"Token not found for {symbol} in MASTER file")


# ________________________________________________________________
#   LOAD MASTER 
# ----------------------------------------------------------------

    def load_master(self):

        for item in self._symbol_generator():

            try:

                exchange = item.get("segment")
                token = str(item.get("token"))
                symbol_name = item.get("symbol")
                symbol = item.get("trading_symbol")
                inst_type=item.get("instrument_type")
                expiry = item.get("expiry")
                opt_type = item.get("option_type")


                if not exchange or not symbol or not token:
                    continue

                symbol = symbol.upper().strip()

                self.symbol_to_token[(exchange, symbol)] = token
                self.token_to_symbol[(exchange, token)] = symbol
                self.symbol_token_map[(symbol)] = token

            except Exception:
                continue

        self._loaded = True

# ============================================================
#   CORE LOOKUPS 
# ============================================================

    def get_token(self, exchange, symbol):

        symbol = symbol.upper()
        return self.symbol_to_token.get((exchange, symbol))


    def get_symbol(self, exchange, token):

        token = str(token)
        return self.token_to_symbol.get((exchange, token))


# =============== Custom Built Option Symbol fetcher ================  (new add-on)

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
            return (exchange, strike_symbol)
        else:
            raise Exception(f"Token not found for {symbol} in MASTER file")


# =============  EXPIRY LOGIC  ==============

    def _get_weekly_expiry(self):

        today = datetime.now().date()

        days_to_thursday = (3 - today.weekday()) % 7
        expiry = today + timedelta(days=days_to_thursday)

        if days_to_thursday == 0:
            expiry += timedelta(days=7)

        return expiry.strftime("%d%b%y").upper()


# ============================================================
#   ATM OPTION REGISTRATION    ( Old Function Logic )
# ============================================================

    def register_atm_options(self, engine, symbol, exchange, strike_dist):

        if not self._loaded:
            raise Exception("TokenRegistry not initialized")

        symbol = symbol.upper()

        spot_token = self.get_token(exchange, symbol)

        if not spot_token:
            raise Exception(f"Spot token not found for {symbol}")

        spot_price = engine.get_ltp_rest(exchange, spot_token)

        if spot_price is None:
            raise Exception(f"Spot price unavailable for {symbol}")

        strike = int(round(float(spot_price) / strike_dist) * strike_dist)

        expiry = self._get_weekly_expiry()

        ce_symbol = f"{symbol}{expiry}C{strike}"
        pe_symbol = f"{symbol}{expiry}P{strike}"

        ce_token = self.get_token("NFO", ce_symbol)
        pe_token = self.get_token("NFO", pe_symbol)

        if not ce_token or not pe_token:
            raise Exception(f"Option tokens not found: {ce_symbol}, {pe_symbol}")

        self.register_instrument("NFO", ce_symbol, "OPT")
        self.register_instrument("NFO", pe_symbol, "OPT")

        return (ce_token, pe_token)


# ============================================================
#   WS READY PAIR (UNCHANGED)
# ============================================================

    def get_ws_instrument(self, exchange, symbol):

        token = self.get_token(exchange, symbol)

        if token:
            return (exchange, token)

        return None


# ============================================================
#   REVERSE ACCESS (UNCHANGED)
# ============================================================

    def get_by_token(self, token):

        token = str(token)

        for (exchange, tk), symbol in self.token_to_symbol.items():
            if tk == token:
                return {
                    "exchange": exchange,
                    "symbol": symbol,
                    "token": token
                }

        return None


# ============================================================
#   REGISTER INSTRUMENT   (PlaceHolder Function)
# ============================================================

    def register_instrument(self, exchange, symbol, symbol_type):

        if not self._loaded:
            raise Exception("TokenRegistry not initialized. Call load_master() first.")

        symbol = symbol.upper().strip()

        token = self.get_token(exchange, symbol)

        if not token:
            raise Exception(f"Token not found for {exchange}:{symbol}")

        key = (exchange, token)

        if key in self._registered_keys:
            return (exchange, token)

        self._registered_keys.add(key)

        ws_key = f"{exchange}|{token}"

        entry = {
            "symbol": symbol,
            "exchange": exchange,
            "symbol_type": symbol_type,
            "token": token,
            "ws_key": ws_key
        }

        TOKEN_BUS.append(entry)

        return (exchange, token)



# ============================================================
#   LEGACY PLACEHOLDERS (STRICT PROTOCOL)
# ============================================================

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


    def get_current_future(self, symbol):
        return None





#_#_#_#
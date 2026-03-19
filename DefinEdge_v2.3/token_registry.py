# ============================================================
# TOKEN REGISTRY v6.3
# Production Stable | Deterministic Ready | No Lambda
# ============================================================

from collections import defaultdict
from datetime import datetime

from edgeAPI_helper import Load_Master


# ============================================================
# INSTRUMENT OBJECT
# ============================================================

class Instrument:

    def __init__(
        self,
        symbol,
        exchange,
        token,
        insttype=None,
        exchange_segment=None,
        security_id=None,
        expiry=None,
        strike=None,
        option_type=None
    ):

        self.symbol = symbol.upper()
        self.exchange = exchange
        self.token = str(token)

        self.insttype = insttype

        self.exchange_segment = exchange_segment or exchange
        self.security_id = str(security_id).strip() if security_id else str(token)

        self.expiry = expiry

        try:
            self.strike = float(strike) if strike not in (None, "", "0") else None
        except Exception:
            self.strike = None

        self.option_type = option_type

    def __repr__(self):

        return (
            f"Instrument("
            f"{self.symbol}, "
            f"type={self.insttype}, "
            f"expiry={self.expiry}, "
            f"strike={self.strike}, "
            f"token={self.token})"
        )


# ============================================================
# TOKEN REGISTRY
# ============================================================

class TokenRegistry:

    MASTER_URL = "https://app.definedgesecurities.com/public/allmaster.zip"

    def __init__(self, api=None):

        self.api = api

        self.token_map = {}
        self.symbol_map = {}
        self.security_map = {}

        self.insttype_map = defaultdict(list)

        self.option_chain_map = defaultdict(list)
        self.option_lookup = {}

        self.futures_map = defaultdict(list)
        self.index_spot_map = {}

        self.df_master = None


# ============================================================
# LOAD MASTER
# ============================================================

    def load_master(self):

        column_names = [
            'SEGMENT','TOKEN','SYMBOL','TRADINGSYM','INSTRUMENT TYPE',
            'EXPIRY','TICKSIZE','LOTSIZE','OPTIONTYPE','STRIKE',
            'PRICEPREC','MULTIPLIER','ISIN','PRICEMULT','UNKNOWN'
        ]

        df = Load_Master()

        # Remove SERIAL column
        df = df.iloc[:, 1:]

        df.columns = column_names

        # Normalize
        df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
        df["SEGMENT"] = df["SEGMENT"].astype(str).str.strip()
        df["OPTIONTYPE"] = df["OPTIONTYPE"].astype(str).str.upper().str.strip()
        df["TRADINGSYM"] = df["TRADINGSYM"].astype(str).str.strip()
        df["INSTRUMENT TYPE"] = df["INSTRUMENT TYPE"].astype(str).str.upper().str.strip()

        self.df_master = df

        # Build maps
        for row in df.itertuples(index=False):

            try:

                symbol = row.SYMBOL
                exchange = row.SEGMENT
                token = str(row.TOKEN)
                security_id = row.TRADINGSYM

                insttype = None
                if hasattr(row, "_asdict"):
                    insttype = row._asdict().get("INSTRUMENT TYPE")

                expiry = self._parse_expiry(row.EXPIRY)
                strike = row.STRIKE
                option_type = row.OPTIONTYPE

                inst = Instrument(
                    symbol,
                    exchange,
                    token,
                    insttype,
                    exchange,
                    security_id,
                    expiry,
                    strike,
                    option_type
                )

                self._register(inst)

            except Exception:
                continue

        self._finalize_maps()


# ============================================================
# REGISTER INSTRUMENT
# ============================================================

    def _register(self, inst):

        self.token_map[str(inst.token)] = inst
        self.security_map[str(inst.security_id)] = inst

        if inst.insttype:
            self.insttype_map[inst.insttype].append(inst)

        # Spot
        if inst.strike is None and inst.expiry is None:

            self.symbol_map[(inst.exchange, inst.symbol)] = inst.token

            if inst.insttype and "IDX" in inst.insttype:
                self.index_spot_map[inst.symbol] = inst.token

        # Options
        if inst.option_type in ("CE", "PE") and inst.strike is not None:

            key = (inst.symbol, inst.expiry)

            if inst.strike not in self.option_chain_map[key]:
                self.option_chain_map[key].append(inst.strike)

            self.option_lookup[
                (inst.symbol, inst.expiry, float(inst.strike), inst.option_type)
            ] = inst.token

        # Futures
        if inst.strike is None and inst.expiry and inst.option_type in ("", None):

            self.futures_map[inst.symbol].append(inst)


# ============================================================
# FINALIZE MAPS
# ============================================================

    def _finalize_maps(self):

        for key in self.option_chain_map:
            self.option_chain_map[key].sort()

        for symbol in self.futures_map:

            futures = self.futures_map[symbol]
            futures.sort(key=self._expiry_sort_key)


    def _expiry_sort_key(self, inst):

        if inst.expiry:
            return inst.expiry

        return datetime.max.date()


# ============================================================
# EXPIRY PARSER
# ============================================================

    def _parse_expiry(self, expiry):

        if expiry in ("", None):
            return None

        try:
            expiry = str(expiry).zfill(8)
            return datetime.strptime(expiry, "%d%m%Y").date()
        except Exception:
            return None


# ============================================================
# BASIC LOOKUPS
# ============================================================

    def get_by_token(self, token):
        return self.token_map.get(str(token))


    def get_by_security_id(self, security_id):
        return self.security_map.get(str(security_id))


# ============================================================
# SYMBOL TOKEN (STRICT)
# ============================================================

    def get_token(self, exchange, symbol):

        symbol = symbol.upper()
        return self.symbol_map.get((exchange, symbol))


# ============================================================
# INDEX SPOT TOKEN
# ============================================================

    def get_index_spot_token(self, symbol):

        symbol = symbol.upper()

        token = self.index_spot_map.get(symbol)

        if token:
            return token

        for inst in self.token_map.values():

            if (
                inst.symbol == symbol
                and inst.strike is None
                and inst.expiry is None
            ):
                return inst.token

        return None


# ============================================================
# OPTION TOKEN (DIRECT)
# ============================================================

    def get_option_token(self, symbol, expiry, strike, option_type):

        if strike is None:
            return None

        return self.option_lookup.get(
            (symbol.upper(), expiry, float(strike), option_type)
        )


# ============================================================
# STRIKE LIST
# ============================================================

    def get_strikes(self, symbol, expiry):

        return self.option_chain_map.get((symbol.upper(), expiry), [])


# ============================================================
# FUTURES
# ============================================================

    def get_futures(self, symbol):

        return self.futures_map.get(symbol.upper(), [])


    def get_current_future(self, symbol):

        futures = self.get_futures(symbol)

        if not futures:
            return None

        return futures[0]


    def get_next_future(self, symbol):

        futures = self.get_futures(symbol)

        if len(futures) < 2:
            return None

        return futures[1]


    def get_far_future(self, symbol):

        futures = self.get_futures(symbol)

        if len(futures) < 3:
            return None

        return futures[2]

    





#_#_#_
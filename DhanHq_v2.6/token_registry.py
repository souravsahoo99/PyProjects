# ============================================================
# TOKEN REGISTRY v1.1
# Production Grade (Dhan Compatible)
# ============================================================

import csv
from collections import defaultdict
from datetime import datetime


# ============================================================
# INSTRUMENT OBJECT
# ============================================================

class Instrument:

    def __init__(
        self,
        symbol,
        exchange,
        token,
        exchange_segment=None,
        security_id=None,
        expiry=None,
        strike=None,
        option_type=None
    ):

        # ----------------------------------------------------
        # GENERIC ENGINE IDENTIFIERS
        # ----------------------------------------------------

        self.symbol = symbol
        self.exchange = exchange
        self.token = str(token)

        # ----------------------------------------------------
        # DHAN IDENTIFIERS
        # ----------------------------------------------------

        self.exchange_segment = exchange_segment or exchange
        self.security_id = security_id or str(token)

        # ----------------------------------------------------
        # DERIVATIVE ATTRIBUTES
        # ----------------------------------------------------

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
            f"expiry={self.expiry}, "
            f"strike={self.strike}, "
            f"type={self.option_type}, "
            f"token={self.token})"
        )


# ============================================================
# TOKEN REGISTRY
# ============================================================

class TokenRegistry:

    def __init__(self):

        # ----------------------------------------------------
        # PRIMARY LOOKUPS
        # ----------------------------------------------------

        # securityId/token → Instrument
        self.token_map = {}

        # (exchange, symbol) → securityId
        self.symbol_map = {}

        # securityId → Instrument (fast lookup)
        self.security_map = {}

        # ----------------------------------------------------
        # OPTIONS REGISTRY
        # ----------------------------------------------------

        # (symbol, expiry) → strike list
        self.option_chain_map = defaultdict(list)

        # (symbol, expiry, strike, type) → securityId
        self.option_lookup = {}

        # ----------------------------------------------------
        # FUTURES REGISTRY
        # ----------------------------------------------------

        self.futures_map = defaultdict(list)


    # ========================================================
    # LOAD INSTRUMENT MASTER
    # ========================================================

    def load_master(self, filepath):

        with open(filepath, "r", encoding="utf-8") as f:

            reader = csv.DictReader(f)

            for row in reader:

                symbol = row.get("symbol")
                exchange = row.get("exchange")

                token = row.get("token")
                security_id = row.get("security_id")

                exchange_segment = row.get("exchange_segment")

                expiry = row.get("expiry")
                strike = row.get("strike")
                option_type = row.get("option_type")

                inst = Instrument(
                    symbol,
                    exchange,
                    token,
                    exchange_segment,
                    security_id,
                    expiry,
                    strike,
                    option_type
                )

                # ------------------------------------------------
                # PRIMARY MAPS
                # ------------------------------------------------

                self.token_map[inst.token] = inst
                self.security_map[inst.security_id] = inst

                # ------------------------------------------------
                # SYMBOL LOOKUP
                # ------------------------------------------------

                if inst.strike is None and inst.expiry in (None, ""):

                    self.symbol_map[(inst.exchange, inst.symbol)] = inst.security_id

                # ------------------------------------------------
                # OPTIONS REGISTRY
                # ------------------------------------------------

                if inst.option_type in ("CE", "PE") and inst.strike is not None:

                    key = (inst.symbol, inst.expiry)

                    if inst.strike not in self.option_chain_map[key]:

                        self.option_chain_map[key].append(inst.strike)

                    self.option_lookup[
                        (inst.symbol, inst.expiry, inst.strike, inst.option_type)
                    ] = inst.security_id

                # ------------------------------------------------
                # FUTURES REGISTRY
                # ------------------------------------------------

                if inst.option_type is None and inst.strike is None and inst.expiry:

                    self.futures_map[inst.symbol].append(inst)

        # ----------------------------------------------------
        # SORT STRIKES
        # ----------------------------------------------------

        for key in self.option_chain_map:

            self.option_chain_map[key].sort()

        # ----------------------------------------------------
        # SORT FUTURES
        # ----------------------------------------------------

        for symbol in self.futures_map:

            self.futures_map[symbol].sort(
                key=lambda x: self._parse_expiry(x.expiry)
            )


    # ========================================================
    # EXPIRY PARSER
    # ========================================================

    def _parse_expiry(self, expiry):

        try:
            return datetime.strptime(expiry, "%Y-%m-%d")
        except Exception:
            return datetime.min


    # ========================================================
    # TOKEN LOOKUP
    # ========================================================

    def get_by_token(self, token):

        return self.token_map.get(str(token))


    def get_by_security_id(self, security_id):

        return self.security_map.get(str(security_id))


    # ========================================================
    # SYMBOL LOOKUP
    # ========================================================

    def get_security_id(self, exchange, symbol):

        return self.symbol_map.get((exchange, symbol))


    # ========================================================
    # OPTION TOKEN
    # ========================================================

    def get_option_security_id(self, symbol, expiry, strike, option_type):

        return self.option_lookup.get(
            (symbol, expiry, float(strike), option_type)
        )


    # ========================================================
    # STRIKE LIST
    # ========================================================

    def get_strikes(self, symbol, expiry):

        return self.option_chain_map.get((symbol, expiry), [])


    # ========================================================
    # ATM STRIKE
    # ========================================================

    def get_atm_strike(self, symbol, expiry, spot):

        if spot is None:
            return None

        strikes = self.get_strikes(symbol, expiry)

        if not strikes:
            return None

        return min(strikes, key=lambda x: abs(x - spot))


    # ========================================================
    # STRIKE WINDOW
    # ========================================================

    def get_strike_window(self, symbol, expiry, spot, window=5):

        strikes = self.get_strikes(symbol, expiry)

        if not strikes:
            return []

        atm = self.get_atm_strike(symbol, expiry, spot)

        if atm is None:
            return []

        idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - atm))

        start = max(idx - window, 0)
        end = min(idx + window + 1, len(strikes))

        return strikes[start:end]


    # ========================================================
    # OPTION UNIVERSE
    # ========================================================

    def build_option_universe(self, symbol, expiry, spot, window=5):

        strikes = self.get_strike_window(symbol, expiry, spot, window)

        instruments = []

        for strike in strikes:

            ce_id = self.get_option_security_id(symbol, expiry, strike, "CE")
            pe_id = self.get_option_security_id(symbol, expiry, strike, "PE")

            if ce_id:
                instruments.append(self.security_map[ce_id])

            if pe_id:
                instruments.append(self.security_map[pe_id])

        return instruments


    # ========================================================
    # FUTURES
    # ========================================================

    def get_futures(self, symbol):

        return self.futures_map.get(symbol, [])


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





#_#_
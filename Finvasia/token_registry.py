# ============================================================
# TOKEN REGISTRY
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
        exchange,
        symbol,
        token,
        expiry=None,
        strike=None,
        option_type=None
    ):

        self.exchange = exchange
        self.symbol = symbol
        self.token = str(token)

        self.expiry = expiry
        self.strike = float(strike) if strike not in (None, "", "0") else None
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
        # CORE REGISTRIES
        # ----------------------------------------------------

        # token -> Instrument
        self.token_map = {}

        # (exchange, symbol) -> token
        self.symbol_map = {}

        # ----------------------------------------------------
        # OPTIONS REGISTRY
        # ----------------------------------------------------

        # (symbol, expiry) -> [strikes]
        self.option_chain_map = defaultdict(list)

        # (symbol, expiry, strike, option_type) -> token
        self.option_lookup = {}

        # ----------------------------------------------------
        # FUTURES REGISTRY
        # ----------------------------------------------------

        # symbol -> [Instrument futures sorted by expiry]
        self.futures_map = defaultdict(list)

    # ========================================================
    # LOAD INSTRUMENT MASTER
    # ========================================================

    def load_master(self, filepath):

        with open(filepath, "r") as f:

            reader = csv.DictReader(f)

            for row in reader:

                exchange = row.get("exchange")
                symbol = row.get("symbol")
                token = row.get("token")

                expiry = row.get("expiry")
                strike = row.get("strike")
                option_type = row.get("option_type")

                inst = Instrument(
                    exchange,
                    symbol,
                    token,
                    expiry,
                    strike,
                    option_type
                )

                # ------------------------------------------------
                # TOKEN MAP
                # ------------------------------------------------

                self.token_map[inst.token] = inst

                # ------------------------------------------------
                # SYMBOL MAP (equities / index)
                # ------------------------------------------------

                if inst.strike is None and inst.expiry in (None, ""):
                    self.symbol_map[(inst.exchange, inst.symbol)] = inst.token

                # ------------------------------------------------
                # OPTIONS REGISTRY
                # ------------------------------------------------

                if inst.option_type in ("CE", "PE"):

                    key = (inst.symbol, inst.expiry)

                    self.option_chain_map[key].append(inst.strike)

                    self.option_lookup[
                        (inst.symbol, inst.expiry, inst.strike, inst.option_type)
                    ] = inst.token

                # ------------------------------------------------
                # FUTURES REGISTRY
                # ------------------------------------------------

                if inst.option_type is None and inst.strike is None and inst.expiry:

                    self.futures_map[inst.symbol].append(inst)

        # ----------------------------------------------------
        # SORT OPTION STRIKES
        # ----------------------------------------------------

        for key in self.option_chain_map:

            self.option_chain_map[key] = sorted(
                list(set(self.option_chain_map[key]))
            )

        # ----------------------------------------------------
        # SORT FUTURES BY EXPIRY
        # ----------------------------------------------------

        for symbol in self.futures_map:

            self.futures_map[symbol].sort(
                key=lambda x: self._parse_expiry(x.expiry)
            )

    # ========================================================
    # INTERNAL EXPIRY PARSER
    # ========================================================

    def _parse_expiry(self, expiry):

        try:
            return datetime.strptime(expiry, "%Y-%m-%d")
        except:
            return datetime.min

    # ========================================================
    # TOKEN LOOKUP
    # ========================================================

    def get_by_token(self, token):

        return self.token_map.get(str(token))

    # ========================================================
    # SYMBOL LOOKUP
    # ========================================================

    def get_token(self, exchange, symbol):

        return self.symbol_map.get((exchange, symbol))

    # ========================================================
    # OPTION TOKEN LOOKUP
    # ========================================================

    def get_option_token(self, symbol, expiry, strike, option_type):

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

        idx = strikes.index(atm)

        start = max(idx - window, 0)
        end = idx + window + 1

        return strikes[start:end]

    # ========================================================
    # OPTION UNIVERSE
    # ========================================================

    def build_option_universe(
        self,
        symbol,
        expiry,
        spot,
        window=5
    ):

        strikes = self.get_strike_window(symbol, expiry, spot, window)

        instruments = []

        for strike in strikes:

            ce_token = self.get_option_token(
                symbol,
                expiry,
                strike,
                "CE"
            )

            pe_token = self.get_option_token(
                symbol,
                expiry,
                strike,
                "PE"
            )

            if ce_token:
                instruments.append(self.token_map[ce_token])

            if pe_token:
                instruments.append(self.token_map[pe_token])

        return instruments

    # ========================================================
    # FUTURES LIST
    # ========================================================

    def get_futures(self, symbol):

        return self.futures_map.get(symbol, [])

    # ========================================================
    # CURRENT FUTURE
    # ========================================================

    def get_current_future(self, symbol):

        futures = self.get_futures(symbol)

        if not futures:
            return None

        return futures[0]

    # ========================================================
    # NEXT FUTURE
    # ========================================================

    def get_next_future(self, symbol):

        futures = self.get_futures(symbol)

        if len(futures) < 2:
            return None

        return futures[1]

    # ========================================================
    # FAR FUTURE
    # ========================================================

    def get_far_future(self, symbol):

        futures = self.get_futures(symbol)

        if len(futures) < 3:
            return None

        return futures[2]
    


#_
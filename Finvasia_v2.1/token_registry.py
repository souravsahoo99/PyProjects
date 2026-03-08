# ============================================================
# TOKEN REGISTRY
# ============================================================

import csv
from collections import defaultdict


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
        self.token = token

        self.expiry = expiry
        self.strike = float(strike) if strike not in (None, "") else None
        self.option_type = option_type

    def __repr__(self):

        return (
            f"Instrument("
            f"{self.symbol}, "
            f"{self.expiry}, "
            f"{self.strike}, "
            f"{self.option_type}, "
            f"{self.token})"
        )


# ============================================================
# TOKEN REGISTRY
# ============================================================

class TokenRegistry:

    def __init__(self):

        # token -> instrument
        self.token_map = {}

        # (exchange, symbol) -> token
        self.symbol_map = {}

        # (symbol, expiry) -> sorted strike list
        self.option_chain_map = defaultdict(list)

        # (symbol, expiry, strike, type) -> token
        self.option_lookup = {}

    # ========================================================
    # LOAD INSTRUMENT MASTER
    # ========================================================

    def load_master(self, filepath):

        with open(filepath, "r") as f:

            reader = csv.DictReader(f)

            for row in reader:

                exchange = row["exchange"]
                symbol = row["symbol"]
                token = row["token"]

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

                # ------------------------------------------
                # TOKEN MAP
                # ------------------------------------------

                self.token_map[token] = inst

                # ------------------------------------------
                # SYMBOL MAP
                # ------------------------------------------

                self.symbol_map[(exchange, symbol)] = token

                # ------------------------------------------
                # OPTION CHAIN
                # ------------------------------------------

                if inst.strike is not None:

                    key = (symbol, expiry)

                    self.option_chain_map[key].append(inst.strike)

                    self.option_lookup[
                        (symbol, expiry, inst.strike, inst.option_type)
                    ] = token

        # ------------------------------------------
        # SORT STRIKES FOR FAST ATM DISCOVERY
        # ------------------------------------------

        for key in self.option_chain_map:

            self.option_chain_map[key] = sorted(
                list(set(self.option_chain_map[key]))
            )

    # ========================================================
    # TOKEN LOOKUP
    # ========================================================

    def get_by_token(self, token):

        return self.token_map.get(token)

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
            (symbol, expiry, strike, option_type)
        )

    # ========================================================
    # GET STRIKE LIST
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
    # STRIKE WINDOW (ATM ± window)
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
    # BUILD ACTIVE OPTION SET
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

# ============================================================
# TOKEN REGISTRY v9.2
# Global Bus Enabled | Production Ready | Deterministic
# ============================================================

from global_token_bus import globalTokenMap


# ============================================================
# TOKEN REGISTRY
# ============================================================

class TokenRegistry:

    def __init__(self, api=None):

        self.api = api

        # ----------------------------------------------------
        # CORE MAPS
        # ----------------------------------------------------

        self.symbol_to_token = {}      # (exchange, symbol) → token
        self.token_to_symbol = {}      # (exchange, token) → symbol
        self.symbol_token_map = {}     # symbol → token (shortcut)

        self._loaded = False

        # ----------------------------------------------------
        # GLOBAL REGISTRY TRACKING
        # ----------------------------------------------------

        self._registered_keys = set()   # (exchange, token)


# ============================================================
# LOAD FROM OFFICIAL DEFINEEDGE SOURCE
# ============================================================

    def load_master(self):

        if not self.api or not hasattr(self.api, "c2i"):
            raise Exception("API not initialized with DefineEdge session")

        symbols = getattr(self.api.c2i, "symbols", None)

        if not symbols:
            raise Exception("DefineEdge symbols not available")

        for item in symbols:

            try:
                exchange = item.get("segment")
                symbol = item.get("trading_symbol")
                token = str(item.get("token"))

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

    def get_token(self, exchange, symbol):

        symbol = symbol.upper()
        return self.symbol_to_token.get((exchange, symbol))


    def get_symbol(self, exchange, token):

        token = str(token)
        return self.token_to_symbol.get((exchange, token))


# ============================================================
# CORE FUNCTION: REGISTER + GLOBAL PUBLISH
# ============================================================

    def register_instrument(self, exchange, symbol, symbol_type):

        if not self._loaded:
            raise Exception("TokenRegistry not initialized. Call load_master() first.")

        symbol = symbol.upper().strip()

        token = self.get_token(exchange, symbol)

        if not token:
            raise Exception(f"Token not found for {exchange}:{symbol}")

        key = (exchange, token)

        # ----------------------------------------------------
        # PREVENT DUPLICATES
        # ----------------------------------------------------

        if key in self._registered_keys:
            return (exchange, token)

        self._registered_keys.add(key)

        # ----------------------------------------------------
        # BUILD GLOBAL ENTRY
        # ----------------------------------------------------

        ws_key = f"{exchange}|{token}"

        entry = {
            "symbol": symbol,
            "exchange": exchange,
            "symbol_type": symbol_type,
            "token": token,
            "ws_key": ws_key
        }

        globalTokenMap.append(entry)

        return (exchange, token)


# ============================================================
# WS READY PAIR
# ============================================================

    def get_ws_instrument(self, exchange, symbol):

        token = self.get_token(exchange, symbol)

        if token:
            return (exchange, token)

        return None


# ============================================================
# REVERSE ACCESS
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


    def get_by_security_id(self, security_id):
        return self.get_token("NSE", security_id)


# ============================================================
# LEGACY COMPATIBILITY (NO-OPS / SAFE)
# ============================================================

    def get_index_spot_token(self, symbol):
        return self.symbol_token_map.get(symbol.upper())


    def get_option_token(self, symbol, expiry, strike, option_type):
        return None


    def get_strikes(self, symbol, expiry):
        return []


    def get_futures(self, symbol):
        return []


    def get_current_future(self, symbol):
        return None


    def get_next_future(self, symbol):
        return None


    def get_far_future(self, symbol):
        return None

    





#_#_#_
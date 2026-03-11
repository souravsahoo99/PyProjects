# ============================================================
# HELPER WRAPPER  v1.0
# Broker Wrapper Layer (Dhan Backend)
# ============================================================

import time
import threading
import pandas as pd

from dhanAPI_helper import DhanApi


# ============================================================
# SHOONYA ENGINE (Wrapper preserved)
# ============================================================

class APIEngine:

    def __init__(self, client_id: str, access_token: str):

        # ----------------------------------------------------
        # API CLIENT
        # ----------------------------------------------------

        self.api = DhanApi(client_id, access_token)

        # ----------------------------------------------------
        # TICK CACHE
        # ----------------------------------------------------

        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        # ----------------------------------------------------
        # WEBSOCKET STATE
        # ----------------------------------------------------

        self._is_ws_connected = False
        self._subscribed_instruments = set()

        # ----------------------------------------------------
        # ROUTING MAP
        # ----------------------------------------------------

        self.market_data_map = {}

        self._login()


    # =========================================================
    # LOGIN (Compatibility Stub)
    # =========================================================

    def _login(self):
        """
        Dhan uses token-based authentication.
        Client initialization already authenticates.
        """
        return True


    # =========================================================
    # RETRY WRAPPER
    # =========================================================

    def _retry(self, func, *args, retries=3, delay=2, **kwargs):

        for attempt in range(retries):

            try:
                return func(*args, **kwargs)

            except Exception:

                if attempt == retries - 1:
                    raise

                time.sleep(delay)


    # =========================================================
    # REST OHLC
    # =========================================================

    def get_ohlc(self, exchange: str, token: str, interval: int = 1):

        raw = self._retry(
            self.api.Get_Intraday_Data,
            security_id=token,
            exchange_segment=exchange,
            instrument_type=interval
        )

        if raw is None:
            return None

        df = pd.DataFrame(raw)

        if df.empty:
            return None

        df = df.rename(columns={
            'time': 'timestamp',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df[['timestamp','open','high','low','close','volume']]


    # =========================================================
    # REST LTP
    # =========================================================

    def get_ltp_rest(self, exchange: str, token: str):

        data = self._retry(
            self.api.Get_LTP,
            security_id=token,
            exchange_segment=exchange
        )

        if data is None:
            return None

        try:
            return float(data['lp'])
        except Exception:
            return None


    # =========================================================
    # WEBSOCKET CALLBACK
    # =========================================================

    def _on_quote(self, message):

        exchange = message.get("exchange_segment")
        token = message.get("security_id")

        key = f"{exchange}|{token}"

        with self._tick_lock:
            self._tick_cache[key] = message

        md = self.market_data_map.get(key)

        if md is None:
            return

        try:

            price = float(message["lp"])

            if md.tick_queue.full():
                return

            md.tick_queue.put_nowait(price)

        except Exception:
            pass


    def _on_open(self):

        self._is_ws_connected = True


    # =========================================================
    # WEBSOCKET CONTROL
    # =========================================================

    def start_ws(self):

        self._is_ws_connected = False

        self.api.Start_Websocket(
            subscribe_callback=self._on_quote
        )

        self._is_ws_connected = True


    def subscribe(self, exchange: str, token: str):

        instrument = (exchange, token)

        self._subscribed_instruments.add(instrument)

        self.api.Subscribe_inst([instrument])


    def wait_for_ws(self, timeout: float = 5.0):

        start = time.time()

        while not self._is_ws_connected:

            if time.time() - start > timeout:
                raise TimeoutError("WebSocket connection timeout")

            time.sleep(0.05)


    def restart_ws(self):

        print("[WS] Restarting WebSocket...")

        self.close_ws()

        time.sleep(1)

        self.start_ws()

        self.wait_for_ws()

        for instrument in self._subscribed_instruments:

            self.api.Subscribe_inst([instrument])

        print("[WS] Reconnected and resubscribed.")


    # =========================================================
    # LIVE LTP
    # =========================================================

    def get_ltp_live(self, exchange: str, token: str):

        key = f"{exchange}|{token}"

        with self._tick_lock:

            msg = self._tick_cache.get(key)

            if msg:

                try:
                    return float(msg['lp'])
                except Exception:
                    return None

        return None


    # =========================================================
    # BEST LTP
    # =========================================================

    def get_best_ltp(self, exchange, token):

        ltp = self.get_ltp_live(exchange, token)

        if ltp:
            return ltp

        return self.get_ltp_rest(exchange, token)


    # =========================================================
    # WEBSOCKET SHUTDOWN
    # =========================================================

    def close_ws(self):

        self.api.Close_Websocket()

        self._is_ws_connected = False


    # =========================================================
    # ENGINE SHUTDOWN
    # =========================================================

    def shutdown(self):

        print("[ENGINE] Shutting down...")

        self.close_ws()

        print("[ENGINE] Shutdown complete.")


# ============================================================
# LTP HELPER
# ============================================================

def get_best_ltp(ws_ltp, rest_ltp):

    try:

        if ws_ltp is not None:

            price = float(ws_ltp)

            if price > 0:
                return price

    except Exception:
        pass

    try:

        if rest_ltp is not None:

            price = float(rest_ltp)

            if price > 0:
                return price

    except Exception:
        pass

    return None


#
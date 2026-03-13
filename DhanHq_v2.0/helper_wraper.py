# ============================================================
# HELPER WRAPPER  v1.2
# Broker Wrapper Layer (Dhan Backend)
# ============================================================

import os
import time
import threading
import pandas as pd

from dhanAPI_helper import DhanApi
from dotenv import find_dotenv, load_dotenv


# ============================================================
#  LOAD ENV
# ============================================================

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)


# ============================================================
# API ENGINE
# ============================================================

class APIEngine:

    def __init__(self):

        client_id = os.getenv("CLIENT_ID")
        access_token = os.getenv("ACCESS_TOKEN")

        self.api = DhanApi(client_id, access_token)

        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._is_ws_connected = False
        self._subscribed_instruments = set()

        self.market_data_map = {}

        self._router_running = False

        self._login()


    # =========================================================
    # LOGIN
    # =========================================================

    def _login(self):

        return True


    # =========================================================
    # RETRY
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

    def get_ohlc(self, exchange, token, interval=1):

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

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors="coerce")

        return df[['timestamp','open','high','low','close','volume']]


    # =========================================================
    # REST LTP
    # =========================================================

    def get_ltp_rest(self, exchange, token):

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
    # WEBSOCKET ROUTER
    # =========================================================

    def _router_loop(self):

        while self._router_running:

            try:

                cache = self.api._tick_cache

                for key, msg in cache.items():

                    md = self.market_data_map.get(key)

                    if md is None:
                        continue

                    try:

                        price = float(msg["lp"])

                        if md.tick_queue.full():

                            try:
                                md.tick_queue.get_nowait()
                            except Exception:
                                pass

                        md.tick_queue.put_nowait(price)

                    except Exception:
                        pass

            except Exception:
                pass

            time.sleep(0.01)


    # =========================================================
    # WEBSOCKET CONTROL
    # =========================================================

    def start_ws(self):

        self.api.Start_Websocket([])

        self._router_running = True

        router = threading.Thread(target=self._router_loop)
        router.daemon = True
        router.start()

        self._is_ws_connected = True


    def subscribe(self, exchange, token):

        instrument = (exchange, token)

        self._subscribed_instruments.add(instrument)

        self.api.Subscribe_inst([instrument])


    def wait_for_ws(self, timeout=5.0):

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

    def get_ltp_live(self, exchange, token):

        key = f"{exchange}|{token}"

        with self.api._tick_lock:

            msg = self.api._tick_cache.get(key)

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
    # SHUTDOWN
    # =========================================================

    def close_ws(self):

        self._router_running = False

        self.api.Close_Websocket()

        self._is_ws_connected = False


    def shutdown(self):

        print("[ENGINE] Shutting down...")

        self.close_ws()

        print("[ENGINE] Shutdown complete.")




#_#
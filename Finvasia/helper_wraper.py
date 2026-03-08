# ============================================================
# SHOONYA ENGINE WRAPPER
# Production Grade
# ============================================================

import os
import time
import pyotp
import asyncio
import threading
import pandas as pd

from ShoonyaAPI_helper import ShoonyaApi
from dotenv import find_dotenv, load_dotenv

# ============================================================
#  LOADING... Environment Variables & Fetching CREDENTIALS 
# ============================================================

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

# ============================================================

user_    = str(os.getenv("USER1"))
pwd_     = str(os.getenv("PWD"))
factor2_ = str(os.getenv("FACTOR2"))
vc_      = str(os.getenv("VC"))
apikey_  = str(os.getenv("APIKEY"))
imei_    = str(os.getenv("IMEI"))

# ============================================================
#      SHOONYA ENGINE
# ============================================================

class ShoonyaEngine:

    def __init__(self):

        self.api = ShoonyaApi()

        # tick cache
        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        # websocket state
        self._is_ws_connected = False
        self._subscribed_instruments = set()

        # routing map
        self.market_data_map = {}

        self._login()


    # =========================================================
    # LOGIN
    # =========================================================

    def _login(self):

        cred = {
            'user': user_,
            'pwd': pwd_,
            'factor2': factor2_,
            'vc': vc_,
            'apikey': apikey_,
            'imei': imei_
        }

        max_retry = 4
        attempt   = 0

        while attempt <= max_retry:

            totp  = pyotp.TOTP(factor2_)
            otp   = totp.now()
            twoFA_= otp

            ret = self.api.Userlogin(
                userid=cred['user'],
                password=cred['pwd'],
                twoFA=twoFA_,
                vendor_code=cred['vc'],
                api_secret=cred['apikey'],
                imei=cred['imei']
            )

            if ret is None:

                print("[ENGINE] Login failed. Retrying...")
                time.sleep(2 + attempt)
                attempt += 1
                continue

            if ret.get("stat") == "Not_Ok":

                print(f"[ENGINE] Login error: {ret.get('emsg')}")
                time.sleep(2 + attempt)
                attempt += 1
                continue

            if ret.get("stat") == "Ok":

                token = ret.get("susertoken")

                if token is None:
                    print("[ENGINE] Session token missing.")
                    attempt += 1
                    continue

                self.api.Set_Session(
                    userid=cred['user'],
                    password=cred['pwd'],
                    usertoken=token
                )

                print("[ENGINE] Login successful.")
                return token

        raise Exception("[ENGINE] Unable to login after retries")


    # =========================================================
    #  RETRY WRAPPER
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
    #  REST OHLC
    # =========================================================

    def get_ohlc(self, exchange: str, token: str, interval: int = 1):

        raw = self._retry(
            self.api.Get_OHLC_data,
            exchange=exchange,
            token=token,
            interval=interval
        )

        if raw is None:
            return None

        df = pd.DataFrame(raw)

        if df.empty:
            return None

        df = df.rename(columns={
            'time': 'timestamp',
            'into': 'open',
            'inth': 'high',
            'intl': 'low',
            'intc': 'close',
            'intv': 'volume'
        })

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df[['timestamp','open','high','low','close','volume']]


    # =========================================================
    #  REST LTP
    # =========================================================

    def get_ltp_rest(self, exchange: str, token: str):

        data = self._retry(
            self.api.Get_Quotes,
            exchange=exchange,
            token=token
        )

        if data is None:
            return None

        return float(data['lp'])


    # =========================================================
    #  WEBSOCKET CALLBACKS
    # =========================================================

    def _on_quote(self, message):

        exchange = message.get("e")
        token = message.get("tk")

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
    #  WEBSOCKET CONTROL
    # =========================================================

    def start_ws(self):

        self._is_ws_connected = False

        self.api.Start_Websocket(
            subscribe_callback=self._on_quote,
            socket_open_callback=self._on_open
        )


    def subscribe(self, exchange: str, token: str):

        instrument = f"{exchange}|{token}"

        self._subscribed_instruments.add(instrument)

        self.api.Subscribe_inst(instrument)


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
            self.api.Subscribe_inst(instrument)

        print("[WS] Reconnected and resubscribed.")


    # =========================================================
    #  LIVE LTP
    # =========================================================

    def get_ltp_live(self, exchange: str, token: str):

        key = f"{exchange}|{token}"

        with self._tick_lock:

            msg = self._tick_cache.get(key)

            if msg:
                return float(msg['lp'])

        return None


    # =========================================================
    #  BEST LTP
    # =========================================================

    def get_best_ltp(self, exchange, token):

        ltp = self.get_ltp_live(exchange, token)

        if ltp:
            return ltp

        return self.get_ltp_rest(exchange, token)


    # =========================================================
    #  WEBSOCKET SHUTDOWN
    # =========================================================

    def close_ws(self):

        self.api.Close_Websocket()

        self._is_ws_connected = False


    # =========================================================
    #  ENGINE SHUTDOWN
    # =========================================================

    def shutdown(self):

        print("[ENGINE] Shutting down...")

        self.close_ws()

        try:
            self.api.logout()
        except Exception:
            pass

        print("[ENGINE] Logout complete.")


# ============================================================
#  LTP HELPER 
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




#_#_#_
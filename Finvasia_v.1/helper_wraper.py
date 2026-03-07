import os
import time
import asyncio
import threading
import pandas as pd

from ShoonyaAPI_helper import ShoonyaApi
from dotenv import find_dotenv, load_dotenv


dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)


# ============================================================
# CREDENTIALS
# ============================================================

user_    = str(os.getenv("USER1"))
pwd_     = str(os.getenv("PWD"))
factor2_ = str(os.getenv("FACTOR2"))
vc_      = str(os.getenv("VC"))
apikey_  = str(os.getenv("APIKEY"))
imei_    = str(os.getenv("IMEI"))


# ============================================================
# SHOONYA ENGINE
# ============================================================

class ShoonyaEngine:

    def __init__(self):

        self.api = ShoonyaApi()

        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._is_ws_connected = False
        self._subscribed_instruments = set()

        # optional external data engine
        self.market_data = None

        self._login()


    # ---------------------------------------------------------
    # LOGIN
    # ---------------------------------------------------------

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
        attempt = 0

        while attempt <= max_retry:

            ret = self.api.Userlogin(userid=cred['user'],password=cred['pwd'],twoFA=cred['factor2'],vendor_code=cred['vc'],api_secret=cred['apikey'],imei=cred['imei'])

            if ret == None:

                print("[ENGINE] Login failed. Retrying...")

                delay = 2 + attempt
                time.sleep(delay)

                attempt += 1

                continue

            elif ret != None :

                if ret.get("stat") == "Not_Ok":
                    print(f"[ENGINE] ! Login Error: {ret.get('emsg', 'Unknown error')}")

                    delay = 2 + attempt
                    time.sleep(delay)

                    attempt += 1

                    continue
                
                elif ret.get("stat") == "Ok" :

                    token = ret.get("susertoken")

                    if token == None:

                        print("[ENGINE] Login succeeded but session Token missing. Retrying...")

                        delay = 2 + attempt
                        time.sleep(delay)

                        attempt += 1

                        continue

                    else:

                        self.api.Set_Session(
                            userid=cred['user'],
                            password=cred['pwd'],
                            usertoken=token
                        )

                        print("[ENGINE] Login successful. Session established.")

                        return token
            else:
                print(f"[ENGINE] Login error: {ret.get("emsg", 'Unknown error')}")

                
        raise Exception("[ENGINE] Unable to login after multiple attempts")


    # ---------------------------------------------------------
    # RETRY WRAPPER
    # ---------------------------------------------------------

    def _retry(self, func, *args, retries=3, delay=2, **kwargs):

        for attempt in range(retries):

            try:

                return func(*args, **kwargs)

            except Exception:

                if attempt == retries - 1:

                    raise

                time.sleep(delay)


    # ---------------------------------------------------------
    # REST OHLC
    # ---------------------------------------------------------

    def get_ohlc(self, exchange: str, token: str, interval: int = 1):

        raw = self._retry(
            self.api.Get_OHLC_data,
            exchange=exchange,
            token=token,
            interval=interval
        )

        df = pd.DataFrame(raw)

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


    # ---------------------------------------------------------
    # REST LTP
    # ---------------------------------------------------------

    def get_ltp_rest(self, exchange: str, token: str):

        data = self._retry(
            self.api.Get_Quotes,
            exchange=exchange,
            token=token
        )

        return float(data['lp'])


    # =========================================================
    # WEBSOCKET
    # =========================================================

    def _on_quote(self, message):

        key = f"{message['e']}|{message['tk']}"

        with self._tick_lock:

            self._tick_cache[key] = message


        # -----------------------------------------------------
        # SURGICAL INTEGRATION → MARKET DATA ENGINE
        # -----------------------------------------------------

        if self.market_data != None:

            try:

                price = float(message["lp"])

                self.market_data.update_tick(price)

            except Exception:

                pass


    def _on_open(self):

        self._is_ws_connected = True


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

        while self._is_ws_connected == False:

            if time.time() - start > timeout:

                raise TimeoutError("WebSocket connection timeout")

            time.sleep(0.05)


    def restart_ws(self):

        print("[WS] Restarting WebSocket...")

        self.close_ws()

        time.sleep(1)

        self.start_ws()

        self.wait_for_ws(timeout=5.0)

        for instrument in self._subscribed_instruments:

            self.api.Subscribe_inst(instrument)

        print("[WS] Reconnected and Resubscribed.")


    def get_ltp_live(self, exchange: str, token: str):

        key = f"{exchange}|{token}"

        with self._tick_lock:

            if key in self._tick_cache:

                return float(self._tick_cache[key]['lp'])

        return None


    def close_ws(self):

        self.api.Close_Websocket()

        self._is_ws_connected = False


    # ---------------------------------------------------------
    # SHUTDOWN
    # ---------------------------------------------------------

    def shutdown(self):

        print("[ENGINE] Shutting down...")

        self.close_ws()

        try:

            self.api.logout()

        except Exception:

            pass

        print("[ENGINE] Logout complete.")



#
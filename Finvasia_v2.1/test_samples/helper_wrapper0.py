import time
import threading
import asyncio
import pandas as pd

from Finvasia.test_samples.api_helper import ShoonyaApiPy


# ============================================================
# SHOONYA ENGINE
# ============================================================

class ShoonyaEngine:
    """
    Core broker interaction layer.
    Handles:
    - Login
    - REST OHLC
    - REST LTP
    - WebSocket ticks
    - Retry mechanism
    """

    def __init__(self, credentials: dict):

        self.api = ShoonyaApiPy()
        self.credentials = credentials

        self._tick_cache = {}
        self._tick_lock = threading.Lock()
        self._is_ws_connected = False

        self._login()

    # -------------------------
    # LOGIN
    # -------------------------

    def _login(self):

        ret = self.api.login(
            userid=self.credentials['user'],
            password=self.credentials['pwd'],
            twoFA=self.credentials['factor2'],
            vendor_code=self.credentials['vc'],
            api_secret=self.credentials['apikey'],
            imei=self.credentials['imei']
        )

        if ret is None:
            raise Exception("Login failed")

    # -------------------------
    # RETRY WRAPPER
    # -------------------------

    def _retry(self, func, *args, retries=3, delay=2, **kwargs):

        for _ in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception:
                time.sleep(delay)

        raise Exception("Operation failed after retries")

    # -------------------------
    # REST — OHLC
    # -------------------------

    def get_ohlc(self, exchange: str, token: str, interval: int = 1):

        raw = self._retry(
            self.api.get_time_price_series,
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

        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

    # -------------------------
    # REST — LTP
    # -------------------------

    def get_ltp_rest(self, exchange: str, token: str):

        data = self._retry(
            self.api.get_quotes,
            exchange=exchange,
            token=token
        )

        return float(data['lp'])

    # -------------------------
    # WEBSOCKET
    # -------------------------

    def _on_quote(self, message):

        key = f"{message['e']}|{message['tk']}"

        with self._tick_lock:
            self._tick_cache[key] = message

    def _on_open(self):
        self._is_ws_connected = True

    def start_ws(self):

        self.api.start_websocket(
            subscribe_callback=self._on_quote,
            socket_open_callback=self._on_open
        )

    def subscribe(self, exchange: str, token: str):

        self.api.subscribe(f"{exchange}|{token}")

    def get_ltp_live(self, exchange: str, token: str):

        key = f"{exchange}|{token}"

        with self._tick_lock:
            if key in self._tick_cache:
                return float(self._tick_cache[key]['lp'])

        return None

    def close_ws(self):

        self.api.close_websocket()
        self._is_ws_connected = False


# ============================================================
#                    REST LTP POLLER
# ============================================================

class RestLTP:
    """
    REST-based LTP polling layer.
    Depends strictly on ShoonyaEngine.
    """

    def __init__(self,
                 engine: ShoonyaEngine,
                 exchange: str,
                 token: str,
                 interval: float = 1.0):

        self.engine = engine
        self.exchange = exchange
        self.token = token
        self.interval = interval

        self._latest_price = None
        self._is_running = False
        self._task = None

    # -------------------------
    # INTERNAL LOOP
    # -------------------------

    async def _poll_loop(self):

        while self._is_running:

            cycle_start = time.time()

            try:
                price = self.engine.get_ltp_rest(
                    self.exchange,
                    self.token
                )

                self._latest_price = price

            except Exception:
                # Engine handles retry internally
                pass

            elapsed = time.time() - cycle_start
            sleep_duration = max(0, self.interval - elapsed)

            await asyncio.sleep(sleep_duration)

    # -------------------------
    # CONTROL METHODS
    # -------------------------

    async def start(self):

        if self._is_running:
            return

        self._is_running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self):

        self._is_running = False

        if self._task:
            await self._task

    # -------------------------
    # DATA ACCESS
    # -------------------------

    def get_latest(self):
        return self._latest_price
    

# ============================================================
#                WEBSOCKET LTP HANDLER
# ============================================================

class WebsocketLTP:
    """
    WebSocket-based LTP handler.
    Depends strictly on ShoonyaEngine.
    """

    def __init__(self,
                 engine: ShoonyaEngine,
                 exchange: str,
                 token: str):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        self._latest_price = None
        self._is_running = False
        self._last_update_time = None
    # -------------------------
    # START STREAM
    # -------------------------

    def start(self):

        if self._is_running:
            return

        self.engine.start_ws()
        self.engine.subscribe(self.exchange, self.token)

        self._is_running = True

    # -------------------------
    # STOP STREAM
    # -------------------------

    def stop(self):

        if not self._is_running:
            return

        self.engine.close_ws()
        self._is_running = False

    # -------------------------
    # DATA ACCESS
    # -------------------------

    def get_latest(self):

        price = self.engine.get_ltp_live(
            self.exchange,
            self.token
        )

        if price is not None:
            self._latest_price = price
            self._last_update_time = time.time()

        return self._latest_price
    
    def last_update_age(self):
        if self._last_update_time is None:
            return None
        return time.time() - self._last_update_time

# ============================================================
#               BEST LTP SELECTOR
# ============================================================ 
def get_best_ltp(ws_ltp: WebsocketLTP,
                 rest_ltp: RestLTP,
                 ws_timeout: float = 3.0):
    """
    Returns best available LTP.

    ws_timeout: seconds after which WS data is considered stale
    """

    ws_price = ws_ltp.get_latest()
    ws_age = ws_ltp.last_update_age()

    # Use WS only if:
    # 1. price exists
    # 2. data is fresh
    if ws_price is not None and ws_age is not None and ws_age <= ws_timeout:
        return ws_price

    # Otherwise fallback
    return rest_ltp.get_latest()
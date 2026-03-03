import time
import asyncio
import threading
import pandas as pd

from Shoonya_API_helper import ShoonyaApi


# ============================================================
#                 SHOONYA ENGINE
# ============================================================

class ShoonyaEngine:

    def __init__(self, credentials: dict):

        self.api = ShoonyaApi()
        self.credentials = credentials

        self._tick_cache = {}
        self._tick_lock = threading.Lock()
        self._is_ws_connected = False
        self._subscribed_instruments = set()

        self._login()

    # -------------------------
    # LOGIN
    # -------------------------

    def _login(self):
        self.api.login(
            userid=self.credentials['user'],
            password=self.credentials['pwd'],
            twoFA=self.credentials['factor2'],
            vendor_code=self.credentials['vc'],
            api_secret=self.credentials['apikey'],
            imei=self.credentials['imei']
        )

    # -------------------------
    # RETRY WRAPPER
    # -------------------------

    def _retry(self, func, *args, retries=3, delay=2, **kwargs):

        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)

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

        self._is_ws_connected = False

        self.api.start_websocket(
            subscribe_callback=self._on_quote,
            socket_open_callback=self._on_open
        )

    def subscribe(self, exchange: str, token: str):

        instrument = f"{exchange}|{token}"
        self._subscribed_instruments.add(instrument)
        self.api.subscribe(instrument)

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
        self.wait_for_ws(timeout=5.0)

        # resubscribe automatically
        for instrument in self._subscribed_instruments:
            self.api.subscribe(instrument)

        print("[WS] Reconnected and Resubscribed.")

    def get_ltp_live(self, exchange: str, token: str):

        key = f"{exchange}|{token}"

        with self._tick_lock:
            if key in self._tick_cache:
                return float(self._tick_cache[key]['lp'])

        return None

    def close_ws(self):

        self.api.close_websocket()
        self._is_ws_connected = False

    # -------------------------
    # SHUTDOWN
    # -------------------------

    def shutdown(self):

        print("[ENGINE] Shutting down...")
        self.close_ws()
        try:
            self.api.logout()
        except:
            pass
        print("[ENGINE] Logout complete.")


# ============================================================
# REST LTP POLLER
# ============================================================

class RestLTP:

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
        self._failure_count = 0

    async def _poll_loop(self):

        while self._is_running:

            cycle_start = time.time()

            try:
                price = self.engine.get_ltp_rest(
                    self.exchange,
                    self.token
                )

                self._latest_price = price
                self._failure_count = 0

            except Exception as e:
                self._failure_count += 1
                print(f"[REST ERROR] {e} | Failures: {self._failure_count}")

                # Cooldown after too many failures
                if self._failure_count >= 10:
                    print("[REST] Cooling down for 10 seconds...")
                    await asyncio.sleep(10)
                    self._failure_count = 0

            elapsed = time.time() - cycle_start
            sleep_duration = max(0, self.interval - elapsed)

            try:
                await asyncio.sleep(sleep_duration)
            except asyncio.CancelledError:
                break

    async def start(self):

        if self._is_running:
            return

        self._is_running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._poll_loop())

    async def stop(self):

        self._is_running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def get_latest(self):
        return self._latest_price


# ============================================================
# WEBSOCKET LTP HANDLER
# ============================================================

class WebsocketLTP:

    def __init__(self,
                 engine: ShoonyaEngine,
                 exchange: str,
                 token: str):

        self.engine = engine
        self.exchange = exchange
        self.token = token

        self._latest_price = None
        self._last_update_time = None
        self._is_running = False

    def start(self):

        if self._is_running:
            return

        self.engine.start_ws()
        self.engine.wait_for_ws(timeout=5.0)
        self.engine.subscribe(self.exchange, self.token)

        self._is_running = True

    def stop(self):

        if not self._is_running:
            return

        self.engine.close_ws()
        self._is_running = False

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
# BEST LTP SELECTOR WITH AUTO-HEAL
# ============================================================

def get_best_ltp(ws_ltp: WebsocketLTP,
                 rest_ltp: RestLTP,
                 ws_timeout: float = 3.0):

    ws_price = ws_ltp.get_latest()
    ws_age = ws_ltp.last_update_age()

    # If WS fresh → use it
    if ws_price is not None and ws_age is not None and ws_age <= ws_timeout:
        return ws_price

    # If WS stale → attempt recovery
    if ws_age is not None and ws_age > ws_timeout:
        print("[WS] Stale data detected. Attempting restart...")
        ws_ltp.engine.restart_ws()

    # Fallback to REST
    return rest_ltp.get_latest()
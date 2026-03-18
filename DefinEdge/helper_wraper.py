# ============================================================
# HELPER WRAPPER  v2.2
# Broker Wrapper Layer (DefineEdge Backend)
# Order Execution WebSocket Integrated
# WebSocket Auto-Reconnect Enabled
# REST Tick LTP Fallback Integrated
# ============================================================

from edgeAPI_helper import EdgeApi

import os
import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from retry import retry
from dotenv import find_dotenv, load_dotenv


dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)


# ============================================================
# API ENGINE
# ============================================================

class APIEngine:

    def __init__(self):

        api_token = os.getenv("EDGE_API_TOKEN")
        api_secret = os.getenv("EDGE_API_SECRET")

        self.api = EdgeApi(api_token, api_secret)

        self._is_ws_connected = False
        self._subscribed_instruments = set()

        self.market_data_map = {}
        self._router_running = False

        # -----------------------------------------------------
        # ORDER STREAM BUFFER
        # -----------------------------------------------------

        self._order_buffer = {}
        self._order_lock = threading.Lock()
        self._order_stream_running = False

        # -----------------------------------------------------
        # WS HEALTH MONITOR
        # -----------------------------------------------------

        self._ws_monitor_running = False
        self._last_tick_time = time.time()

    # =========================================================
    # REST OHLC
    # =========================================================

    @retry(tries=5, delay=2, backoff=2)
    def get_ohlc(self, exchange, token, interval="min"):

        now = datetime.now()
        days_ago = now - timedelta(days=7)
        start_date = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)

        raw = self.api.Get_Intraday_Data(
            trading_symbol=token,
            exchange=exchange,
            timeframe=interval,
            start=start_date,
            end=now
        )

        if raw is None:
            return None

        df = pd.DataFrame(list(raw))

        if df.empty:
            return None

        # DefineEdge SDK returns "datetime"
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "timestamp"})

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        return df[["timestamp", "open", "high", "low", "close", "volume"]]


    # =========================================================
    # REST QUOTE LTP
    # =========================================================

    @retry(tries=5, delay=2, backoff=2)
    def get_ltp_rest(self, exchange, token):

        data = self.api.Get_LTP(
            trading_symbol=token,
            exchange=exchange
        )

        if data is None:
            return None

        try:
            return float(data.get("lp"))
        except Exception:
            return None


    # =========================================================
    # REST TICK LTP
    # =========================================================

    @retry(tries=3, delay=1, backoff=2)
    def get_ltp_tick_rest(self, exchange, token):

        try:

            now = datetime.now()
            start = now - timedelta(seconds=10)

            return self.api.Get_Tick_Data(
                trading_symbol=token,
                exchange=exchange,
                start=start,
                end=now
            )

        except Exception:
            return None


    # =========================================================
    # WEBSOCKET ROUTER
    # =========================================================

    def _router_loop(self):

        while self._router_running:

            try:

                with self.api._tick_lock:
                    cache = dict(self.api._tick_cache)

                if cache:
                    self._last_tick_time = time.time()

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
    # ORDER STREAM ROUTER
    # =========================================================

    def _order_router(self):

        while self._order_stream_running:

            try:

                with self.api._order_lock:
                    buffer = dict(self.api._order_buffer)

                if not buffer:
                    time.sleep(0.01)
                    continue

                for order_id, status in buffer.items():

                    with self._order_lock:
                        self._order_buffer[order_id] = status

            except Exception:
                pass

            time.sleep(0.01)


    # =========================================================
    # WS HEALTH MONITOR
    # =========================================================

    def _ws_monitor(self):

        while self._ws_monitor_running:

            try:

                if not self._is_ws_connected:
                    time.sleep(1)
                    continue

                idle_time = time.time() - self._last_tick_time

                if idle_time > 15:

                    print("[WS] Tick stream stalled. Restarting...")

                    self.restart_ws()

                    self._last_tick_time = time.time()

            except Exception:
                pass

            time.sleep(5)


    # =========================================================
    # WEBSOCKET CONTROL
    # =========================================================

    @retry(tries=5, delay=2, backoff=2)
    def start_ws(self):

        self.api.Start_Websocket()

        self.api.Start_Order_Stream()

        # -----------------------------------------------------

        self._order_stream_running = True
        order_router = threading.Thread(target=self._order_router, daemon=True)
        order_router.start()

        # -----------------------------------------------------

        self._router_running = True
        router = threading.Thread(target=self._router_loop, daemon=True)
        router.start()

        # -----------------------------------------------------

        self._ws_monitor_running = True
        monitor = threading.Thread(target=self._ws_monitor, daemon=True)
        monitor.start()

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
                    return float(msg["lp"])
                except Exception:
                    return None

        return None


    # =========================================================
    # BEST LTP
    # =========================================================

    def get_best_ltp(self, exchange, token):

        # 1. WebSocket LTP
        ltp = self.get_ltp_live(exchange, token)
        if ltp:
            return ltp

        # 2. REST Tick LTP
        ltp = self.get_ltp_tick_rest(exchange, token)
        if ltp:
            return ltp

        # 3. REST Quote LTP
        return self.get_ltp_rest(exchange, token)


    # =========================================================
    # ORDER CONFIRMATION HELPER
    # =========================================================

    def confirm_order_execution(self, order_id):

        if order_id is None:
            return False

        order_id = str(order_id)

        with self._order_lock:
            status = self._order_buffer.get(order_id)

        if status:

            status = status.upper()

            if status in ["TRADED", "FILLED", "COMPLETE", "EXECUTED"]:
                return True

        return False


    # =========================================================
    # SHUTDOWN
    # =========================================================

    def close_ws(self):

        self._router_running = False
        self._order_stream_running = False
        self._ws_monitor_running = False

        self.api.Close_Websocket()

        self._is_ws_connected = False


    def shutdown(self):

        print("[ENGINE] Shutting down...")

        self.close_ws()

        print("[ENGINE] Shutdown complete.")





#_#
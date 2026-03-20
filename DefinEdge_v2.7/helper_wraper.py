# ============================================================
# HELPER WRAPPER v3.1
# Broker Wrapper Layer (DefineEdge Backend)
# Production Safe Version
# Thread Safe + WS safe
# ============================================================

from edgeAPI_helper import EdgeApi

import os
import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from retry import retry
from dotenv import find_dotenv, load_dotenv


dotenv_file = find_dotenv()
load_dotenv(dotenv_file)
api_token = os.getenv("EDGE_API_TOKEN")
api_secret = os.getenv("EDGE_API_SECRET")


# ============================================================
# API ENGINE
# ============================================================

class APIEngine():

    def __init__(self):

        self.api = EdgeApi(api_token, api_secret)

        self.market_data_map = {}

        # ----------------------------------------------------
        # WEBSOCKET STATE
        # ----------------------------------------------------

        self._is_ws_connected = False
        self._subscribed_instruments = set()

        # ----------------------------------------------------
        # THREAD FLAGS
        # ----------------------------------------------------

        self._router_running = False
        self._order_stream_running = False
        self._ws_monitor_running = False

        # ----------------------------------------------------
        # ORDER BUFFER
        # ----------------------------------------------------

        self._order_buffer = {}
        self._order_lock = threading.Lock()

        # ----------------------------------------------------
        # WS HEALTH
        # ----------------------------------------------------

        self._last_tick_time = time.time()


# ============================================================
# REST data (UNCHANGED)
# ============================================================

    @retry(tries=3, delay=2, backoff=2)
    def get_ohlc(self, exchange, token, start, interval="min"):

        now = datetime.now()

        raw = self.api.Get_Intraday_Data(
            exchange=exchange,
            trading_symbol=token,
            timeframe=interval,
            start=start,
            end=now
        )

        return pd.DataFrame(list(raw))


    @retry(tries=3, delay=1, backoff=2)
    def get_ltp_rest(self, exchange, token):

        data = self.api.Get_LTP(exchange=exchange, trading_symbol=token)

        try:
            return float(data.get("lp"))
        except Exception:
            return None


    @retry(tries=3, delay=1, backoff=2)
    def get_tick_data_rest(self, exchange, token):

        now = datetime.now()
        start = now - timedelta(seconds=60)

        price = self.api.Get_Tick_Data(
            exchange=exchange,
            trading_symbol=token,
            start=start,
            end=now
        )

        if price is None:
            return None

        return pd.DataFrame(list(price))


# ============================================================
# WEBSOCKET ROUTER (UNCHANGED)
# ============================================================

    def _router_loop(self):

        while self._router_running:

            try:

                with self.api._tick_lock:
                    cache = self.api._tick_cache.copy()

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

            time.sleep(0.03)


# ============================================================
# ORDER ROUTER (UNCHANGED)
# ============================================================

    def _order_router(self):

        while self._order_stream_running:

            try:

                with self.api._order_lock:
                    buffer = self.api._order_buffer.copy()

                if not buffer:
                    time.sleep(0.02)
                    continue

                for order_id, status in buffer.items():

                    with self._order_lock:
                        self._order_buffer[str(order_id)] = str(status).upper()

            except Exception:
                pass

            time.sleep(0.02)


# ============================================================
# WS MONITOR (UNCHANGED LOGIC)
# ============================================================

    def _ws_monitor(self):

        while self._ws_monitor_running:

            try:

                if not self._is_ws_connected:
                    time.sleep(1)
                    continue

                idle = time.time() - self._last_tick_time

                if idle > 15:
                    
                    print("[WS] Tick stream stalled. Restarting...")
                    
                    self.restart_ws()
                    self._last_tick_time = time.time()

            except Exception:
                pass

            time.sleep(5)


# ============================================================
# WEBSOCKET CONTROL (FIXED)
# ============================================================

    @retry(tries=5, delay=2, backoff=2)
    def start_ws(self):

        #  Ensure subscriptions prepared BEFORE start
        for inst in self._subscribed_instruments:
            self.api.Subscribe_inst([inst])

        # Start WS (non-blocking)
        self.api.Start_Websocket()

        #  WAIT FOR ACTUAL WS LOGIN (CRITICAL FIX)
        start = time.time()
        while not self.api._ws_logged_in:
            if time.time() - start > 10:
                raise TimeoutError("WS login timeout")
            time.sleep(0.05)

        # Threads start AFTER WS ready
        self._router_running = True
        threading.Thread(target=self._router_loop, daemon=True).start()

        self._order_stream_running = True
        threading.Thread(target=self._order_router, daemon=True).start()

        self._ws_monitor_running = True
        threading.Thread(target=self._ws_monitor, daemon=True).start()

        self._is_ws_connected = True


    def subscribe(self, exchange, token):

        instrument = (exchange, token)

        self._subscribed_instruments.add(instrument)

        # Always register in broker (deferred-safe)
        self.api.Subscribe_inst([instrument])


    def wait_for_ws(self, timeout=5.0):

        start = time.time()

        while not self._is_ws_connected:

            if time.time() - start > timeout:
                raise TimeoutError("WebSocket connection timeout")

            time.sleep(0.1)


    def restart_ws(self):

        print("[WS] Restarting WebSocket...")

        self.close_ws()
        time.sleep(1)

        self.start_ws()

        print("[WS] Reconnected (subscriptions auto-restored).")


# ============================================================
# LIVE LTP (UNCHANGED)
# ============================================================

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


# ============================================================
# BEST LTP (UNCHANGED)
# ============================================================

    def get_best_ltp(self, exchange, token):

        ltp = self.get_ltp_live(exchange, token)
        if ltp is not None:
            return ltp

        return self.get_ltp_rest(exchange, token)


# ============================================================
# ORDER CONFIRMATION (UNCHANGED)
# ============================================================

    def confirm_order_execution(self, order_id):

        order_id = str(order_id)

        with self._order_lock:
            status = self._order_buffer.get(order_id)

        if status:

            status = status.upper()

            if status in ["TRADED", "FILLED", "COMPLETE", "EXECUTED"]:
                return True

        else: 
            status = self.api.Get_Order_By_ID(order_id)


# ============================================================
# SHUTDOWN ( SAFE FIX )
# ============================================================

    def close_ws(self):

        self._router_running = False
        self._order_stream_running = False
        self._ws_monitor_running = False

        try:
            self.api.Close_Websocket()
        except Exception:
            pass

        self._is_ws_connected = False


    def shutdown(self):

        print("[ENGINE] Shutting down API layer...")
        self.close_ws()
        print("[ENGINE] API layer shutdown complete.")






#_#_#_#_
# ============================================================
# HELPER WRAPPER v4.2
# Broker Wrapper Layer (DefineEdge Backend)
# Unified Token Flow Integrated
# Thread Safe + WS safe
# ============================================================
from token_registry import TokenRegistry
from dhanAPI_helper import DhanApi

import os
import time 
import threading
import pandas as pd
from retry import retry
from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv


dotenv_file = find_dotenv()
load_dotenv(dotenv_file)

client_id = os.getenv("DHAN_CLIENT_ID")
access_token = os.getenv("DHAN_ACCESS_TOKEN")

# ============================================================
#     API - ENGINE                                                
# ============================================================

class APIEngine():

    def __init__(self):

        self.api = DhanApi(client_id, access_token)

        # TOKEN REGISTRY Integration (New) 
        self.registry = TokenRegistry(api=self.api)
        #self.registry.load_master()

        self.market_data_map = {}

        # WEBSOCKET STATE
        self._is_ws_connected = False
        self._subscribed_instruments: set[tuple[str,str]] = set() 
                                                                        ## [("NSE", "26000"),("NSE", "26009"),("NFO", "66022"),("NFO", "66023")]
        # THREAD FLAGS
        self._router_running = False
        self._order_stream_running = False
        self._ws_monitor_running = False

        # ORDER BUFFER
        self._order_buffer = {}
        self._order_lock = threading.Lock()

        # WS HEALTH
        self._last_tick_time = time.time()

# ============================================================
#  UNIFIED TOKEN FLOW (CORE)
# ============================================================

    def _get_token(self, exchange, trading_symbol):
        return self.registry.get_token(exchange, trading_symbol)


    def resolve_and_subscribe(self, exchange, trading_symbol):
        
        token = self._get_token(exchange, trading_symbol)

        self.subscribe(exchange, token)

        return token

# ============ Resolute Exchange as per Broker ==============

    def _resolute_exchange(self,exchange):        

        if   exchange in ["NSE", "Nse", "nse"]:
            return self.api.c2i.EXCHANGE_TYPE_NSE
        elif exchange in ["BSE", "Bse", "bse"]:
            return self.api.c2i.EXCHANGE_TYPE_BSE
        elif exchange in ["NFO", "Nfo", "nfo"]:
            return self.api.c2i.EXCHANGE_TYPE_NFO
        elif exchange in ["BFO", "Bfo", "bfo"]:
            return self.api.c2i.EXCHANGE_TYPE_BFO
        elif exchange in ["MCX", "Mcx", "mcx"]:
            return self.api.c2i.EXCHANGE_TYPE_MCX            
        elif exchange in ["CDS" , "Cds", "cds"]:
            return self.api.c2i.EXCHANGE_TYPE_CDS
        else:
            raise Exception (f"{exchange} is not valid. Use NSE, BSE, NFO, BFO, MCX or CDS.")


    def _resolute_ordertype(self,order_type):

        if   order_type in ["B", "BUY", "buy", "Buy"]:
            return self.api.c2i.ORDER_TYPE_BUY
        elif order_type in ["S","SELL","sell","Sell"]:
            return self.api.c2i.ORDER_TYPE_SELL
        else:
            raise Exception (f"{order_type} is not valid. Use BUY or SELL.")

# ============================================================
#   REST  DATA 
# ============================================================

    @retry(tries=3, delay=2, backoff=2)
    def get_ohlc(self, exchange, trade_sym, start, interval="min"):
        exc= self._resolute_exchange(exchange)
        now = datetime.now()

        raw = self.api.Get_Intraday_Data(
            exchange=exc,
            trading_symbol=trade_sym,
            timeframe=interval,
            start=start,
            end=now
        )

        return pd.DataFrame(list(raw))


    @retry(tries=3, delay=1, backoff=2)
    def get_tick_data_rest(self, exchange, trade_sym):
        exc= self._resolute_exchange(exchange)
        now = datetime.now()
        start = now - timedelta(minutes=1)

        price = self.api.Get_Tick_Data(
            exchange=exc,
            trading_symbol=trade_sym,
            start=start,
            end=now
        )

        if price is None:
            return None

        return pd.DataFrame(list(price))


    @retry(tries=1, delay=1, backoff=1)
    def get_ltp_rest(self, exchange, trade_sym):
        exc = self._resolute_exchange(exchange)
        price = self.api.Get_LTP(exchange=exc, trading_symbol=trade_sym)
        if price is None:
            return None
        else:
            return price
        
# ============================================================
#   REST  - ORDER PLACING  
# ============================================================

    def place_market(self,exchange,order_type,trading_symbol,quantity:int):

        Exchange_ = self._resolute_exchange(exchange)
        Order_type = self._resolute_ordertype(order_type)

        order = self.api.Place_Order(
            exchange=Exchange_,
            order_type=Order_type,
            price=0.0,
            price_type=self.api.c2i.PRICE_TYPE_MARKET,
            product_type=self.api.c2i.PRODUCT_TYPE_NORMAL,
            quantity=quantity,
            tradingsymbol=trading_symbol,
        )
        order_id = order.get("order_id")
        status = order.get("status")

        if status == "SUCCESS":
            return order_id
        else:
            raise Exception ("Market_Order not Placed")

    # =========== LIMIT ORDER ===========

    def place_limit(self,exchange,order_type,trading_symbol,quantity:int,price:float):

        Exchange_ = self._resolute_exchange(exchange)
        Order_type = self._resolute_ordertype(order_type)

        order = self.api.Place_Order(
            exchange=Exchange_,
            order_type=Order_type,
            price=price,
            price_type=self.api.c2i.PRICE_TYPE_LIMIT,
            product_type=self.api.c2i.PRODUCT_TYPE_NORMAL,
            quantity=quantity,
            tradingsymbol=trading_symbol,
        )
        order_id = order.get("order_id")
        status = order.get("status")

        if status == "SUCCESS":
            return order_id
        else:
            raise Exception ("Limit_Order not Placed")
        
# ============================================================
#   ORDER CONFIRMATION and Cancellation (REST Based)
# ============================================================

    def confirm_order_execution(self, order_id):

        order_ID = str(order_id)

        if order_id is not None: 
            req = self.api.Get_Order_By_ID(order_ID)

            result = req.get("status")

            if result == "SUCCESS":
                return True
            else:
                return False


    def cancel_order_by_id(self, order_id):

        order_ID = str(order_id)

        if order_id is not None: 
            req = self.api.Cancel_Order(order_ID)

            result = req.get("status")

            if result == "SUCCESS":
                return True
            else:
                return False

# ============================================================
#  WEBSOCKET ROUTER (UNCHANGED)
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

            time.sleep(0.05)


# ============================================================
#  WEBSOCKET - ORDER ROUTER 
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

            time.sleep(0.3)


# ============================================================
#  WS - MONITOR 
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
#  WEBSOCKET CONTROLs 
# ============================================================


    def subscribe(self, exchange, token):

        instrument = (exchange, token)

        if instrument not in self._subscribed_instruments:

            self._subscribed_instruments.add(instrument)

            Exchange_ = self._resolute_exchange(exchange)
            self.api.Subscribe_inst(Exchange_, token)

    def wait_for_ws(self, timeout=5.0):

        start = time.time()

        while not self._is_ws_connected:

            if time.time() - start > timeout:
                raise TimeoutError("WebSocket connection timeout")

            time.sleep(0.2)

    def close_ws(self):

        self._router_running = False
        self._order_stream_running = False
        self._ws_monitor_running = False

        try:
            self.api.Close_Websocket()
        except Exception:
            pass

        self._is_ws_connected = False
        time.sleep(1)

    def start_ws(self):

        # Start WS (non-blocking)
        if not self._subscribed_instruments:
            raise Exception ("Instruments are not subscribed")
        else:
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
        print("[WS] Web-Socket thread_ started.")


    def restart_ws(self):
        print("[WS] Restarting WebSocket...")

        self.close_ws()
        time.sleep(1)
        self.start_ws()

        print("[WS] Reconnected (subscriptions restored).")


# ============================================================
#  LIVE LTP  (WS)                             
# ============================================================

    def get_ltp_live(self, exchange, token):
        
        Exchange_ = self._resolute_exchange(exchange)
        key = f"{Exchange_}|{token}"

        with self.api._tick_lock:

            msg = self.api._tick_cache.get(key)

            if msg:                                # Logic could be improve
                try:
                    return float(msg["lp"])
                except Exception:
                    return None


# ============================================================
#  GET BEST LTP (FIXED)
# ============================================================

    def get_best_ltp(self, exchange, token,):

        ltp:float = self.get_ltp_live(exchange, token)
        
        if ltp is None:

            trade_sym = self.registry.get_symbol(exchange, token)

            if not trade_sym:
                raise Exception ("trade_symbol not found")
            price = self.get_ltp_rest(exchange, trade_sym)
            return float(price)
        
        else: 
            return ltp


# ============================================================
#  SHUTDOWN 
# ============================================================

    def shutdown(self):
        print("[ENGINE] Shutting down API layer...")
        
        self.close_ws()
        
        print("[ENGINE] API layer shutdown complete.")




#_#_#_#_#_#_#
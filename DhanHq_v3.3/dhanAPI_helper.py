# ============================================================
# DHAN API HELPER  v1.1
# Superset Broker Wrapper
# ============================================================
from dhanhq import DhanContext
from dhanhq import dhanhq
from dhanhq import marketfeed
from dhanhq import orderupdate

import threading
import logging
import time
from typing import Optional, Dict, List


logger = logging.getLogger(__name__)

#logging.basicConfig(level=logging.DEBUG)

# ============================================================
# ORDER OBJECT (Official Dhan Format)
# ============================================================

class Order:

    def __init__(self,security_id: str ,exchange_segment ,transaction_type ,quantity: int ,order_type,product_type ,price: float = 0.0):

        self.security_id = security_id
        self.exchange_segment = exchange_segment
        self.transaction_type = transaction_type
        self.quantity = quantity
        self.order_type = order_type
        self.product_type = product_type
        self.price = price

# ============================================================
#  DHAN_hq API Class 
# ============================================================

class DhanApi:

    def __init__(self, client_id: str, access_token: str):

        self.client_id = client_id
        self.access_token = access_token

        # REST client
        self.client = dhanhq(client_id, access_token)

        # websocket state
        self.market_feed = None
        self.order_feed = None

        self._ws_running = False
        self._tick_cache = {}
        self._tick_lock = threading.Lock()

        self._subscribed = set()


    # ========================================================
    # AUTHENTICATION
    # ========================================================

    def login(self):
        """
        Dhan uses access token authentication.
        Client is already initialized.
        """
        return True


    def logout(self):
        """
        placeholder for compatibility
        """
        return True


    # ========================================================
    # ENGINE COMPATIBILITY METHODS
    # ========================================================

    # --------------------------------------------------------
    # PLACE ORDER
    # --------------------------------------------------------

    def Place_Order(self, order: Order):

        return self.client.place_order(security_id=order.security_id,exchange_segment=order.exchange_segment,transaction_type=order.transaction_type,quantity=order.quantity,order_type=order.order_type,product_type=order.product_type,price=order.price)


    # --------------------------------------------------------
    # MODIFY ORDER
    # --------------------------------------------------------

    def Modify_Order(self,order_id,order_type,leg_name,quantity,price,trigger_price,disclosed_quantity,validity):

        return self.client.modify_order(order_id,order_type,leg_name,quantity,price,trigger_price,disclosed_quantity,validity)


    # --------------------------------------------------------
    # CANCEL ORDER
    # --------------------------------------------------------

    def Cancel_Order(self, order_id):

        return self.client.cancel_order(order_id)


    # --------------------------------------------------------
    # GET POSITIONS
    # --------------------------------------------------------

    def Get_Positions(self):

        return self.client.get_positions()

    # --------------------------------------------------------
    # GET ORDERBOOK
    # --------------------------------------------------------

    def Get_Orderbook(self):

        return self.client.get_order_list()


    # --------------------------------------------------------
    # GET TRADEBOOK
    # --------------------------------------------------------

    def Get_TradeBook(self):

        return self.client.get_trade_book()
    
    # --------------------------------------------------------
    # Get Order detail by [ Order ID ]
    # --------------------------------------------------------
    def Get_Order_By_ID(self,order_id):

        return self.client.get_order_by_id(order_id)
    # --------------------------------------------------------
    def Get_Tbook_By_Orderid(self,order_id):

        return self.client.get_trade_book(order_id)

    # ========================================================
    # MARKET DATA [ REST ]
    # ========================================================

    def Get_LTP(self, security_id, exchange_segment):

        data = self.client.ohlc_data(securities={exchange_segment: [security_id]})

        return data


    def Get_Intraday_Data(self, security_id, exchange_segment, instrument_type):

        return self.client.intraday_minute_data(security_id,exchange_segment,instrument_type)


    def Get_Daily_Data(self,security_id,exchange_segment,instrument_type,expiry_code,from_date,to_date):

        return self.client.historical_daily_data(security_id,exchange_segment,instrument_type,expiry_code,from_date,to_date)


    # ========================================================
    # WEBSOCKET - MARKET FEED
    # ========================================================

    def Start_Websocket(self, instruments: List):

        self.market_feed = marketfeed.DhanFeed(self.client_id,self.access_token,instruments,"v2")

        self._ws_running = True

        thread = threading.Thread(target=self._run_market_feed)
        thread.daemon = True
        thread.start()


    def _run_market_feed(self):

        while self._ws_running:

            try:
                self.market_feed.run_forever()

                data = self.market_feed.get_data()

                if data:
                    self._handle_tick(data)

            except Exception as e:

                logger.error(f"MarketFeed error: {e}")
                time.sleep(2)


    def _handle_tick(self, data):

        key = f"{data.get('exchange_segment')}|{data.get('security_id')}"

        with self._tick_lock:
            self._tick_cache[key] = data


    def Subscribe_inst(self, instruments):

        if self.market_feed:
            self.market_feed.subscribe_symbols(instruments)


    def Unsubscribe_inst(self, instruments):

        if self.market_feed:
            self.market_feed.unsubscribe_symbols(instruments)


    def Close_Websocket(self):

        self._ws_running = False

        if self.market_feed:
            self.market_feed.disconnect()


    # ========================================================
    # ORDER UPDATE WEBSOCKET
    # ========================================================

    def Start_Order_Stream(self):

        self.order_feed = orderupdate.OrderSocket(self.client_id,self.access_token)

        thread = threading.Thread(target=self._run_order_feed)
        thread.daemon = True
        thread.start()


    def _run_order_feed(self):

        while True:

            try:
                self.order_feed.connect_to_dhan_websocket_sync()

            except Exception as e:

                logger.error(f"OrderSocket error: {e}")
                time.sleep(5)


    # ========================================================
    # NATIVE DHAN FUNCTIONS (Superset Layer)
    # ========================================================

    def fetch_security_list(self, mode="compact"):

        return self.client.fetch_security_list(mode)


    def expiry_list(self, under_security_id, under_exchange_segment):

        return self.client.expiry_list(under_security_id,under_exchange_segment)


    def option_chain(self, under_security_id, under_exchange_segment, expiry):

        return self.client.option_chain(under_security_id,under_exchange_segment,expiry)


    def place_forever(self,security_id,exchange_segment,transaction_type,product_type,order_type,quantity,price,trigger_price):

        return self.client.place_forever(security_id=security_id,exchange_segment=exchange_segment,transaction_type=transaction_type,product_type=product_type,order_type=order_type,quantity=quantity,price=price,trigger_price=trigger_price)


    def get_holdings(self):

        return self.client.get_holdings()


    def get_fund_limits(self):

        return self.client.get_fund_limits()


    def convert_to_date_time(self, epoch):

        return self.client.convert_to_date_time(epoch)


    def generate_tpin(self):

        return self.client.generate_tpin()


    def edis_inquiry(self):

        return self.client.edis_inquiry()
    

   
    

#_#
from NorenRestApiPy.NorenApi import NorenApi
from typing import Optional, List, Dict
import concurrent.futures
import pandas as pd
import threading
import time
import os

from dotenv import find_dotenv
from dotenv import load_dotenv

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

user    = os.getenv("USER")
pwd     = os.getenv("PWD")
factor2 = os.getenv("FACTOR2")
vc      = os.getenv("VC")
apikey  = os.getenv("APIKEY")
imei    = os.getenv("IMEI")     

cred = {
    "user": user,
    "pwd": pwd,
    "factor2": factor2,
    "vc": vc,
    "apikey": apikey,
    "imei": imei
}

def get_time(time_string):
    data = time.strptime(time_string,'%d-%m-%Y %H:%M:%S')

    return time.mktime(data)

class Order:
    """
    Order data container.
    Used for structured order placement.
    """

    def __init__(self,
                 buy_or_sell: str,
                 product_type: str,
                 exchange: str,
                 tradingsymbol: str,
                 quantity: int,
                 price_type: str,
                 price: float = 0.0,
                 trigger_price: Optional[float] = None,
                 discloseqty: int = 0,
                 retention: str = "DAY",
                 remarks: str = "tag"):

        self.buy_or_sell = buy_or_sell
        self.product_type = product_type
        self.exchange = exchange
        self.tradingsymbol = tradingsymbol
        self.quantity = quantity
        self.discloseqty = discloseqty
        self.price_type = price_type
        self.price = price
        self.trigger_price = trigger_price
        self.retention = retention
        self.remarks = remarks

# Original calling method
class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')        
        global api
        api = self

# GPT style calling method
class ShoonyaApi:
    def __init__(self):
        self._api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')
        

    # ==========================================================
    #                       AUTHENTICATION
    # ==========================================================

    def login(self, userid, password, twoFA, vendor_code, api_secret, imei):
        """
        Performs QuickAuth login.
        Must be called before any REST or WS function.
        """
        return self._api.login(userid, password, twoFA,
                               vendor_code, api_secret, imei)

    def set_session(self, userid, password, usertoken):
        """
        Restore session without logging in again.
        """
        return self._api.set_session(userid, password, usertoken)
    
    def logout(self):
        """
        Terminates session.
        """
        return self._api.logout()

    # ==========================================================
    #                     MARKET DATA  ( REST API )
    # ==========================================================

    def get_quotes(self, exchange: str, token: str) -> Dict:
        """
        Fetch live LTP and quote details.
        """
        return self._api.get_quotes(exchange, token)

    def get_time_price_series(self,
                              exchange: str,
                              token: str,
                              interval: int = None,
                              starttime: int = None,
                              endtime: int = None) -> List[Dict]:
        """
        Fetch OHLC data.
        Interval: 1, 3, 5, 15, 30, 60 etc.
        """
        return self._api.get_time_price_series(
            exchange, token, starttime, endtime, interval
        )

    def get_daily_price_series(self,
                               exchange: str,
                               tradingsymbol: str,
                               startdate=None,
                               enddate=None):
        """
        Fetch daily OHLC.
        """
        return self._api.get_daily_price_series(
            exchange, tradingsymbol, startdate, enddate
        )

    # ==========================================================
    #                  ORDER MANAGEMENT
    # ==========================================================

    def place_basket(self, orders):

        resp_err = 0
        resp_ok  = 0
        result   = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            future_to_url = {executor.submit(self.place_order, order): order for order in  orders}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
            try:
                result.append(future.result())
            except Exception as exc:
                print(exc)
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

        return result
        
    #     Places a single order.
        
    def placeOrder(self, order: Order):
        ret = self._api.place_order(self, buy_or_sell=order.buy_or_sell, product_type=order.product_type,
                            exchange=order.exchange, tradingsymbol=order.tradingsymbol, 
                            quantity=order.quantity, discloseqty=order.discloseqty, price_type=order.price_type, 
                            price=order.price, trigger_price=order.trigger_price,
                            retention=order.retention, remarks=order.remarks)
        #print(ret)

        return ret
         
    def place_order(self, order: Order):

        return self._api.place_order(
            buy_or_sell=order.buy_or_sell,
            product_type=order.product_type,
            exchange=order.exchange,
            tradingsymbol=order.tradingsymbol,
            quantity=order.quantity,
            discloseqty=order.discloseqty,
            price_type=order.price_type,
            price=order.price,
            trigger_price=order.trigger_price,
            retention=order.retention,
            remarks=order.remarks
        )

    def modify_order(self,
                     orderno,
                     exchange,
                     tradingsymbol,
                     newquantity,
                     newprice_type,
                     newprice=0.0,
                     newtrigger_price=None):

        return self._api.modify_order(
            orderno, exchange, tradingsymbol,
            newquantity, newprice_type,
            newprice, newtrigger_price
        )

    def cancel_order(self, orderno):

        return self._api.cancel_order(orderno)

    def single_order_history(self, orderno):

        return self._api.single_order_history(orderno)

    def get_order_book(self):

        return self._api.get_order_book()

    def get_trade_book(self):

        return self._api.get_trade_book()

    # ==========================================================
    #                 PORTFOLIO & LIMITS
    # ==========================================================

    def get_positions(self):
        return self._api.get_positions()

    def get_holdings(self):
        return self._api.get_holdings()

    def get_limits(self):
        return self._api.get_limits()

    def span_calculator(self, actid, positions):
        return self._api.span_calculator(actid, positions)

    # ==========================================================
    #                     SEARCH & OPTIONS
    # ==========================================================

    def searchscrip(self, exchange, searchtext):
        return self._api.searchscrip(exchange, searchtext)

    def get_option_chain(self, exchange, tradingsymbol,
                         strikeprice, count=2):
        return self._api.get_option_chain(
            exchange, tradingsymbol, strikeprice, count
        )

    def get_security_info(self, exchange, token):
        return self._api.get_security_info(exchange, token)

    def option_greek(self, expiredate, StrikePrice,
                     SpotPrice, InterestRate,
                     Volatility, OptionType):
        return self._api.option_greek(
            expiredate, StrikePrice, SpotPrice,
            InterestRate, Volatility, OptionType
        )

    # ==========================================================
    #                        WEBSOCKET
    # ==========================================================

    def start_websocket(self,
                        subscribe_callback=None,
                        order_update_callback=None,
                        socket_open_callback=None,
                        socket_close_callback=None,
                        socket_error_callback=None):
        """
        Start live WebSocket connection.
        """
        return self._api.start_websocket(
            subscribe_callback,
            order_update_callback,
            socket_open_callback,
            socket_close_callback,
            socket_error_callback
        )

    def subscribe(self, instrument):
        """
        Subscribe to live ticks.
        """
        return self._api.subscribe(instrument)

    def unsubscribe(self, instrument):
        return self._api.unsubscribe(instrument)

    def close_websocket(self):
        """
        Close live stream.
        """
        return self._api.close_websocket()
    

# create an instance of the wrapper class and then invoke methods on it
api = ShoonyaApi()  
ret = api.login(userid=user,
                password=pwd,
                twoFA=factor2,
                vendor_code=vc,
                api_secret=apikey,  
                imei=imei)

print(ret)  
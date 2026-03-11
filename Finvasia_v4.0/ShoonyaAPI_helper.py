from NorenRestApiPy.NorenApi import NorenApi
from typing import Optional, List, Dict
import concurrent.futures
import threading
import logging
import time


logger = logging.getLogger(__name__)
def reportmsg(msg):
    #print(msg)
    logger.debug(msg)

def reporterror(msg):
    #print(msg)
    logger.error(msg)

def reportinfo(msg):
    #print(msg)
    logger.info(msg)


def get_time(time_string):
    data = time.strptime(time_string,'%d-%m-%Y %H:%M:%S')

    return time.mktime(data)
api=None
class Order:
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

class ShoonyaApi(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')        
        global api
        api = self
        
    #                      [ AUTHENTICATION ]
        
    def Userlogin(self, userid, password, twoFA, vendor_code, api_secret, imei):
        #enable dbug to see request and responses
        logging.basicConfig(level=logging.DEBUG)
        
        ret= super().login(userid=userid, password=password, twoFA=twoFA, vendor_code=vendor_code, api_secret=api_secret, imei=imei)
        return ret
        #returns Dictionary: self._susertoken = resDict['susertoken']
    
    def Set_Session(self, userid, password, usertoken):
        ret= super().set_session(userid=userid, password=password, usertoken=usertoken)
        return ret                      
        #returns boolian Value True or False
        
    
    def logout(self):
        ret= super().logout()
        return ret
        #returns dictionary: resDict['stat'] = 'Ok' if successful, else error message

    #                       [ MARKET DATA ]

    def Get_OHLC_data(self, exchange, token, starttime=None, endtime=None, interval=None):
        Res= super().get_time_price_series(self, exchange=exchange, token=token, starttime=starttime, endtime=endtime, interval=interval)
        return Res
        #returns List: type(resDict) != list : return None, else return resDict

    def Get_Daily_Data(self, exchange, tradingsymbol, startdate=None, enddate=None):
        res= super().get_daily_price_series(self, exchange=exchange, tradingsymbol=tradingsymbol, startdate=startdate, enddate=enddate)
        return res
        #returns List: type(resDict) != list : return None, else return resDict

    #
    #                 [ ORDER MANAGEMENT ]
    #

    def Place_Order(self,order: Order):
        ret= super().place_order(self, buy_or_sell=order.buy_or_sell, product_type=order.product_type,
                            exchange=order.exchange, tradingsymbol=order.tradingsymbol, 
                            quantity=order.quantity, discloseqty=order.discloseqty, price_type=order.price_type, 
                            price=order.price, trigger_price=order.trigger_price,
                            retention=order.retention, remarks=order.remarks)
        
        return ret
        # returns Dict: resDict['stat'] = 'Ok' if successful, else None

    def Modify_Order(self, orderno, exchange, tradingsymbol, newquantity,
                    newprice_type, newprice=0.0, newtrigger_price=None, bookloss_price = 0.0, bookprofit_price = 0.0, trail_price = 0.0):
        ret= super().modify_order(orderno=orderno, exchange=exchange, tradingsymbol=tradingsymbol, newquantity=newquantity, newprice_type=newprice_type, newprice=newprice, newtrigger_price=newtrigger_price, bookloss_price=bookloss_price, bookprofit_price=bookprofit_price, trail_price=trail_price)
        
        return ret
        # returns Dict: resDict['stat'] = 'Ok' if successful, else None

    def Cancel_Order(self, orderno):
        ret = super().cancel_order(self, orderno=orderno)
        return ret
        # returns Dict: resDict['stat'] = 'Ok' if successful, else None 

    def Exit_Order(self, orderno, product_type):
        ret= super().exit_order(self, orderno=orderno, product_type=product_type)
        return ret
        # returns Dict: resDict['stat'] = 'Ok' if successful, else None

    def Get_Positions(self):
        res= super().get_positions(self)
        return res
        # returns List: type(resDict) != list : return None, else return resDict

    def Single_Order_History(self, orderno):
        book= super().single_order_history(self, orderno=orderno)
        return book
        # returns List: type(resDict) != list : return None, else return resDict

    def Get_Orderbook(self):
        book= super().get_order_book(self)
        return book
        # returns List: type(resDict) != list : return None, else return resDict

    def Get_TradeBook(self):
        book= super().get_trade_book(self)
        return book
        # returns List: type(resDict) != list : return None, else return resDict
        
    # 
    #                    [ SEARCH & OPTIONS ]
    # 
    
    def Search_Script(self, exchange, searchtext):
        res= super().searchscrip(exchange=exchange, searchtext=searchtext)
        return res
        #returns Dictionary: resDict['stat'] != 'Ok' : return None, else return resDict

    def Get_Quotes(self, exchange, token):
        res= super().get_quotes(self, exchange=exchange, token=token)
        return res
        #returns Dictionary: resDict['stat'] != 'Ok' : return None, else return resDict

    def Get_Option_Chain(self, exchange, tradingsymbol, strikeprice, count=2):
        getoc= super().get_option_chain(self, exchange=exchange, tradingsymbol=tradingsymbol, strikeprice=strikeprice, count=count)
        return getoc
        #returns Dict: if resDict['stat'] != 'Ok' : return None, else return resDict

    # ==========================================================
    #                        WEBSOCKET
    # ==========================================================

    def Start_Websocket(self, subscribe_callback = None, order_update_callback = None, socket_open_callback = None, socket_close_callback = None, socket_error_callback = None):
        ws= super().start_websocket(self, subscribe_callback=subscribe_callback, order_update_callback=order_update_callback, socket_open_callback=socket_open_callback, socket_close_callback=socket_close_callback, socket_error_callback=socket_error_callback)
        return ws    

    def Close_Websocket(self):
        ws= super().close_websocket(self)
        return ws
    
    #  Subscribe live Market LTP Data for given instrument token. Token can be obtained from searchscrip or get_security_info API.
    def Subscribe_inst(self, Instrument):
        sub= super().subscribe(instrument=Instrument)
        return sub
        
    def Unsubscribe_inst(self, Instrument):
        sub= super().unsubscribe(instrument=Instrument)
        return sub
    
    def Subscribe_order(self):
        sub= super().subscribe_orders()
        return sub
    
   



#_#
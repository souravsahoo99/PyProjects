# ============================================================
# ENGINE DIAGNOSTICS TOOL
# Broker → Wrapper → Data Verification
# Permanent Debug Ground
# ============================================================

from datetime import datetime , timedelta
import time
import os
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from edgeAPI_helper import EdgeApi
from helper_wraper import APIEngine


# ============================================================
# ENV LOAD
# ============================================================

dotenv_file = find_dotenv()
load_dotenv(dotenv_file)

API_TOKEN = os.getenv("EDGE_API_TOKEN")
API_SECRET = os.getenv("EDGE_API_SECRET")


def test_rest_ohlc(Engine):

    print("\n========== TEST 1 : REST OHLC ==========\n")


    days_ago = datetime.now() - timedelta(days=10)
    start_ = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)

    data = Engine.get_ohlc(exchange='NSE',trade_sym='Nifty 50',start=start_ ,interval= "min")

    print("\n***** Fetched OHLC Data *****\n")

    print(data.iloc[-15:])


def test_rest_tick_data(Engine):

    print("\n========== TEST 2 : REST TICK DATA ==========\n")

    data = Engine.get_tick_data_rest(exchange='NSE',trade_sym='Nifty 50')

    print("\n***** Fetched Tick Data *****\n")

    print(data.iloc[-15:])


def test_rest_ltp(Engine):
    
    print("\n========== TEST 3 : REST TICK LTP ==========\n")   

    data = Engine.get_ltp_rest(exchange='NSE', trade_sym='Nifty 50')

    print(data)


# ============================================================
# MAIN MENU
# ============================================================

def main():

    print("\n========== ENGINE DIAGNOSTICS ==========\n")

    engine = APIEngine()

    test_rest_ohlc(engine) 

    test_rest_tick_data(engine)

    while True:

        test_rest_ltp(engine)  
        time.sleep(5)
# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    main()
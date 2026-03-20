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

    print("\n========== TEST 3 : REST OHLC ==========\n")


    days_ago = datetime.now() - timedelta(days=1)
    start_ = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)

    data = Engine.get_ohlc(exchange='NSE',token='Nifty 50',start=start_ ,interval= "min")

    print("\n***** Fetched OHLC Data *****\n")

    print(data.iloc[-15:])


# ============================================================
# MAIN MENU
# ============================================================

def main():

    print("\n========== ENGINE DIAGNOSTICS ==========\n")

    engine = APIEngine()

    test_rest_ohlc(engine)


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    main()
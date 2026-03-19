# ============================================================
# ENGINE DIAGNOSTICS TOOL
# Broker → Wrapper → Data Verification
# Permanent Debug Ground
# ============================================================

import time
import os
from dotenv import load_dotenv, find_dotenv

from edgeAPI_helper import EdgeApi
from helper_wraper import APIEngine


# ============================================================
# ENV LOAD
# ============================================================

dotenv_file = find_dotenv()
load_dotenv(dotenv_file)

API_TOKEN = os.getenv("EDGE_API_TOKEN")
API_SECRET = os.getenv("EDGE_API_SECRET")


# ============================================================
# TEST 1 : BROKER LOGIN
# ============================================================

def test_broker_login():

    print("\n========== TEST 1 : BROKER LOGIN ==========\n")

    api = EdgeApi(API_TOKEN, API_SECRET)

    print("Broker login successful\n")

    return api


# ============================================================
# TEST 2 : REST LTP
# ============================================================

def test_rest_ltp(api):

    print("\n========== TEST 2 : REST LTP ==========\n")

    try:

        data = api.Get_LTP(
            trading_symbol="NIFTY-I",
            exchange="NSE"
        )

        print("REST LTP Response:", data)

    except Exception as e:

        print("REST LTP ERROR:", e)


# ============================================================
# TEST 3 : REST OHLC
# ============================================================

def test_rest_ohlc(api):

    print("\n========== TEST 3 : REST OHLC ==========\n")

    try:

        data = api.Get_Intraday_Data(
            trading_symbol="NIFTY-I",
            exchange="NSE",
            timeframe="min",
            start=None,
            end=None
        )

        count = 0

        for row in data:

            print(row)

            count += 1

            if count >= 5:
                break

    except Exception as e:

        print("REST OHLC ERROR:", e)


# ============================================================
# TEST 4 : MASTER FILE DOWNLOAD
# ============================================================

def test_master_download(api):

    print("\n========== TEST 4 : MASTER FILE ==========\n")

    try:

        df = api.download_master_zip()

        print("Master file rows:", len(df))

    except Exception as e:

        print("MASTER DOWNLOAD ERROR:", e)


# ============================================================
# TEST 5 : WEBSOCKET STREAM
# ============================================================

def test_websocket_stream():

    print("\n========== TEST 5 : WEBSOCKET STREAM ==========\n")

    engine = APIEngine()

    engine.start_ws()

    engine.wait_for_ws()

    print("WebSocket connected\n")

    # --------------------------------------------------------
    # Example tokens
    # Replace with actual tokens from TokenRegistry
    # --------------------------------------------------------

    instruments = [

        ("NSE", "26000"),  # NIFTY
        ("NSE", "26009"),  # BANKNIFTY

    ]

    for inst in instruments:

        print("Subscribing:", inst)

        engine.subscribe(inst[0], inst[1])

    print("\nListening for ticks...\n")

    while True:

        try:

            for key, md in engine.market_data_map.items():

                try:

                    price = md.tick_queue.get_nowait()

                    print("[TICK]", key, price)

                except:
                    pass

        except Exception as e:

            print("Tick read error:", e)

        time.sleep(0.1)


# ============================================================
# MAIN MENU
# ============================================================

def main():

    print("\n========== ENGINE DIAGNOSTICS ==========\n")

    api = test_broker_login()

    test_rest_ltp(api)

    test_rest_ohlc(api)

    test_master_download(api)

    print("\nProceeding to WebSocket test...\n")

    test_websocket_stream()


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    main()
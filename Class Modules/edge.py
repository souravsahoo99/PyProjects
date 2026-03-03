import os
import pyotp
import pandas as pd
from integrate import ConnectToIntegrate, IntegrateData
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv

pd.set_option('display.max_rows', None)

# load env
load_dotenv(find_dotenv())

api_token = os.getenv("API_TOKEN")
api_secret = os.getenv("API_SECRET")
totp_secret = os.getenv("TOTP_SECRET")

if not all([api_token, api_secret, totp_secret]):
    raise ValueError("Missing environment variables")

# login
conn = ConnectToIntegrate()
totp = pyotp.TOTP(totp_secret).now()

try:
    conn.login(api_token=api_token, api_secret=api_secret, totp=totp)
except Exception as e:
    raise RuntimeError(f"Login failed: {e}")

# time window
days_back = int(os.getenv("DAYS_BACK", 90))
start = (datetime.now(timezone.utc) - timedelta(days=days_back)).replace(
    hour=9, minute=15, second=0, microsecond=0
)

# fetch data
ic = IntegrateData(conn)
history = ic.historical_data(
    exchange=os.getenv("EXCHANGE", "NSE"),
    trading_symbol=os.getenv("SYMBOL", "Nifty 50"),
    timeframe=conn.TIMEFRAME_TYPE_DAY,
    start=start,
    end=datetime.now(timezone.utc),
)

df = pd.DataFrame(list(history))

if df.empty:
    raise ValueError("No data returned")

print("\n***** Fetched OHLC Data *****\n")
print(df.iloc[-10:])
import os
import pyotp
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv
from integrate import ConnectToIntegrate, IntegrateData


class MarketDataFetcher:
    """
    Professional market data loader class
    Handles:
    - authentication
    - connection
    - historical data fetch
    """

    def __init__(self):

        load_dotenv(find_dotenv())

        self.api_token = os.getenv("API_TOKEN")
        self.api_secret = os.getenv("API_SECRET")
        self.totp_secret = os.getenv("TOTP_SECRET")

        if not all([self.api_token, self.api_secret, self.totp_secret]):
            raise ValueError("Missing environment variables")

        self.conn = None
        self.data_api = None

        self._login()

    # ---------- LOGIN ----------
    def _login(self):
        """Authenticate broker session"""

        self.conn = ConnectToIntegrate()
        totp = pyotp.TOTP(self.totp_secret).now()

        try:
            self.conn.login(
                api_token=self.api_token,
                api_secret=self.api_secret,
                totp=totp
            )
        except Exception as e:
            raise RuntimeError(f"Login failed: {e}")

        self.data_api = IntegrateData(self.conn)

    # ---------- DATE WINDOW ----------
    def _date_range(self, days_back: int):
        """Return start + end datetime"""
        end = datetime.now(timezone.utc)

        start = (end - timedelta(days=days_back)).replace(
            hour=9, minute=15, second=0, microsecond=0
        )

        return start, end

    # ---------- FETCH ----------
    def fetch(
        self,
        symbol: str,
        exchange: str = "NSE",
        timeframe=None,
        days_back: int = 90
    ) -> pd.DataFrame:
        """
        Fetch historical OHLC data
        """

        if timeframe is None:
            timeframe = self.conn.TIMEFRAME_TYPE_DAY

        start, end = self._date_range(days_back)

        history = self.data_api.historical_data(
            exchange=exchange,
            trading_symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )

        df = pd.DataFrame(list(history))

        if df.empty:
            raise ValueError(f"No data returned for {symbol}")

        return df

    # ---------- LAST CANDLES ----------
    def latest(
        self,
        symbol: str,
        rows: int = 10,
        **kwargs
    ):
        df = self.fetch(symbol, **kwargs)
        return df.tail(rows)
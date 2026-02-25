from core.data_fetcher import MarketDataFetcher

fetcher = MarketDataFetcher()

df = fetcher.fetch("Nifty 50")

print(df.tail(10))
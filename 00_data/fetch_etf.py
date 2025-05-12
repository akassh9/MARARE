#!/usr/bin/env python3
import os
import pandas as pd
import yfinance as yf

def main():
    # 1. Define tickers and date
    TICKERS = ['ACWI','EEM','BNDX','HYLB','DBC','VNQI']
    start_date = '2005-01-01'

    # 2. Download data
    raw = yf.download(
        tickers=TICKERS,
        start=start_date,
        interval='1mo',
        auto_adjust=True,
        progress=False
    )

    # 3. Extract the adjusted-close series in either layout
    if isinstance(raw.columns, pd.MultiIndex):
        # MultiIndex: level 0 might be 'Adj Close' (or 'Close' if auto_adjust renamed it)
        if 'Adj Close' in raw.columns.levels[0]:
            prices = raw['Adj Close']
        else:
            prices = raw['Close']
    else:
        # Single-level: the DataFrame itself is already adjusted prices
        prices = raw

    # 4. Clean up
    prices.index = pd.to_datetime(prices.index)
    prices.index.name = 'Date'

    # 5. Write out
    out_path = os.path.join(os.path.dirname(__file__), 'raw_etf.parquet')
    prices.to_parquet(out_path)
    print(f"Saved raw ETF prices to {out_path}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import yaml
import pandas as pd
from pandas_datareader.data import DataReader
from datetime import datetime

def main():
    # Load metadata
    meta = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "series_meta.yaml")))
    start = "1991-01-01"

    # Fetch each series
    frames = []
    for code in meta:
        if code == "SP500":
            csv_path = "/Users/akash009/MARARE/00_data/sp500_yahoo.csv"
            df = pd.read_csv(csv_path, parse_dates=["Date"])
            df.set_index("Date", inplace=True)
            # rename your column (whatever it was) to the FRED code
            df.columns = ["SP500"]
        else:
            # fetch everything else from FRED
            df = DataReader(code, "fred", start)
            df.columns = [code]
        frames.append(df)

    # Concatenate into one DataFrame
    raw = pd.concat(frames, axis=1)
    raw.index = pd.to_datetime(raw.index)

    # Write to parquet
    out_path = os.path.join(os.path.dirname(__file__), "raw_fred.parquet")
    raw.to_parquet(out_path)
    print(f"Saved raw FRED data to {out_path}")

if __name__ == "__main__":
    main()
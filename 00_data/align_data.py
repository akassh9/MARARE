#!/usr/bin/env python3
import os
import pandas as pd

ME = "M"          # use "ME" if you’re on pandas ≥ 3.0 
ROLL = 36         # rolling‑corr window (months)
CUT_OFF = "2024-12-31"  # truncate all series at end of 2024

def main():
    base = os.path.dirname(__file__)

    fred = pd.read_parquet(os.path.join(base, "raw_fred.parquet"))
    etf  = pd.read_parquet(os.path.join(base, "raw_etf.parquet"))

    # --- 1. Resample to month‑end ------------------------------------------
    fred_m = fred.resample(ME).last()
    etf_m  = etf .resample(ME).last()

    # --- 2. Derive 3‑yr stock/bond correlation ----------------------------
    sp_ret   = fred_m["SP500"].pct_change()
    yld_diff = fred_m["DGS10"].diff()          # 10‑y yield Δ (absolute)
    fred_m["STOCK_BOND_CORR"] = (
        sp_ret.rolling(ROLL).corr(yld_diff)
    )

    # --- 3a. Now truncate macro and prices to desired window -------------
    # Macro: begin 1994-01-31, end 2024-12-31
    fred_m = fred_m.loc["1994-01-31":CUT_OFF]
    # Prices: cap at end of 2024
    etf_m  = etf_m.loc[:CUT_OFF]

    # --- 4. Clean price panel ---------------------------------------------
    # Drop pre-inception rows where all tickers are NaN
    etf_m = etf_m.dropna(how="all")
    # Find first date where none of the tickers are NaN and truncate
    first_full = etf_m.dropna(how="any").index.min()
    etf_m = etf_m.loc[first_full:]

    # --- 5. Persist three separate parquet files --------------------------
    fred_out   = os.path.join(base, "macro_data.parquet")
    prices_out = os.path.join(base, "prices_data.parquet")
    all_out    = os.path.join(base, "all_data.parquet")

    fred_m.to_parquet(fred_out)
    etf_m.to_parquet(prices_out)

    # Outer‑join so early macro rows keep NaN prices,
    # later rows have both.
    all_data = fred_m.join(etf_m, how="outer")
    all_data.to_parquet(all_out)

    print(f"Saved macro   → {fred_out}")
    print(f"Saved prices  → {prices_out}")
    print(f"Saved union   → {all_out}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import pandas as pd

BASE = os.path.dirname(__file__)

# individual file paths
MACRO_PATH   = os.path.join(BASE, "macro_data.parquet")
PRICES_PATH  = os.path.join(BASE, "prices_data.parquet")
ALL_PATH     = os.path.join(BASE, "all_data.parquet")

FRED_COLS = [
    "SP500", "DTB3", "T10Y2Y", "DCOILWTICO",
    "PCOPPUSDM", "VIXCLS", "STOCK_BOND_CORR"
]

ETF_COLS = ["ACWI","EEM","BNDX","HYLB","DBC","VNQI"]

# ------------------------------------------------------------------------

def get_macro():
    """Full‑history macro panel (with STOCK_BOND_CORR)."""
    df = pd.read_parquet(MACRO_PATH)
    idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='ME', name=df.index.name)
    df = df.reindex(idx)
    # Hide the raw 10-year yield series from output
    df = df.drop(columns=["DGS10"])
    return df

def get_prices():
    """
    ETF price DataFrame starting at the first month where *
    all* tickers have data and ending 2024‑12‑31. NaN‑free by construction.
    """
    df = pd.read_parquet(PRICES_PATH)
    idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='ME', name=df.index.name)
    df = df.reindex(idx)
    return df

def get_all():
    """Outer‑joined macro + price DataFrame (NaNs allowed)."""
    df = pd.read_parquet(ALL_PATH)
    idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='ME', name=df.index.name)
    df = df.reindex(idx)
    return df

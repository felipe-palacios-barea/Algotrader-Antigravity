"""
Module: yahoo_recs.py

Fetches analyst recommendations for each ticker in final_trades.csv using the
FinancialModelingPrep API. Adds a 'Recommendation' column in place and
overwrites final_trades.csv.
"""
import os
import time
from pathlib import Path
import pandas as pd
import requests
from config import ENRICHED_TRADES_CSV_PATH

# Polite pause between API calls
time_sleep = 0.5

# FinancialModelingPrep API key
FMP_API_KEY = os.getenv('FMP_API_KEY', '')


def fetch_recommendation_fmp(ticker: str) -> str:
    """
    Fetch the latest analyst rating for the given ticker from FinancialModelingPrep.
    Returns rating string (e.g., 'buy', 'hold', 'sell') or 'N/A' on error.
    """
    if not FMP_API_KEY:
        return 'N/A'
    url = f"https://financialmodelingprep.com/api/v3/rating/{ticker.upper()}"
    params = {'apikey': FMP_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0].get('rating', 'N/A') or 'N/A'
    except Exception:
        pass
    return 'N/A'


def augment_recommendations(
    trades_path: Path = ENRICHED_TRADES_CSV_PATH,
    sleep_sec: float = time_sleep
) -> pd.DataFrame:
    """
    Load final_trades.csv, fetch unique ticker recommendations via FMP,
    add/overwrite 'Recommendation' column, and save back to the same file.
    Returns the updated DataFrame.
    """
    df = pd.read_csv(trades_path, encoding='utf-8-sig', dtype=str)
    tickers = df['Ticker'].dropna().unique()

    rec_map = {}
    for t in tickers:
        rec_map[t] = fetch_recommendation_fmp(t)
        time.sleep(sleep_sec)

    df['Recommendation'] = df['Ticker'].map(rec_map).fillna('N/A')
    df.to_csv(trades_path, index=False, encoding='utf-8-sig')
    return df


if __name__ == '__main__':
    updated = augment_recommendations()
    print(f"Updated {len(updated)} rows in {ENRICHED_TRADES_CSV_PATH} with FMP recommendations.")
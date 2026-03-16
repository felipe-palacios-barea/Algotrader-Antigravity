"""
Module: price_change.py

Fetches current stock prices and variance metrics for each ticker in final_trades.csv
using the FinancialModelingPrep API. Computes:
  - Current_Price: latest quote
  - Pct_Change: (current - bought) / bought * 100
  - Std_Dev: historical standard deviation over the past 3 months (~63 trading days)
  - Std_Dev_Away: how many standard deviations away the current price is from the historical mean
Overwrites final_trades.csv in place.)
  - Std_Dev_Away: how many standard deviations away the current price is from the historical mean
Overwrites final_trades.csv in place.
"""
import os
import time
from pathlib import Path
import pandas as pd
import requests
from typing import Optional
from config import ENRICHED_TRADES_CSV_PATH
import config

SECTOR_ETF_MAP = {
    'Technology': 'XLK',
    'Financial Services': 'XLF',
    'Financial': 'XLF',
    'Healthcare': 'XLV',
    'Consumer Cyclical': 'XLY',
    'Industrials': 'XLI',
    'Energy': 'XLE',
    'Consumer Defensive': 'XLP',
    'Utilities': 'XLU',
    'Real Estate': 'XLRE',
    'Basic Materials': 'XLB',
    'Communication Services': 'XLC'
}

# Polite pause between API calls
TIME_SLEEP = 0.5

# FinancialModelingPrep API key
FMP_API_KEY = os.getenv('FMP_API_KEY', '')
# Ensure fallback from config's env load
if not FMP_API_KEY:
    try:
        from dotenv import load_dotenv
        load_dotenv(config.PROJECT_ROOT / '.env')
        FMP_API_KEY = os.getenv('FMP_API_KEY', '')
    except ImportError:
        pass


def fetch_current_price(ticker: str) -> Optional[float]:
    """
    Fetch the latest stock price for a given ticker from FinancialModelingPrep.
    """
    if not FMP_API_KEY:
        return None
    url = f"https://financialmodelingprep.com/api/v3/quote/{ticker.upper()}"
    params = {'apikey': FMP_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        if isinstance(data, list) and data:
            price = data[0].get('price')
            return float(price) if price is not None else None
    except Exception:
        pass
    return None


def fetch_historical_prices(ticker: str, days: int = 5) -> list[float]:
    """
    Fetch the past `days` closing prices for `ticker` using yfinance to bypass FMP restrictions.
    Returns list of floats or empty list on error.
    """
    try:
        import yfinance as yf
        # Fetch closing prices over the last `days` period
        stock = yf.Ticker(ticker)
        hist = stock.history(period=f"{days}d")
        if not hist.empty:
            prices = hist['Close'].tolist()
            return [float(p) for p in prices if pd.notna(p)]
    except Exception as e:
        print(f"Error fetching yfinance history for {ticker}: {e}")
    return []


def fetch_sector_momentum(ticker: str, days: int = 5) -> Optional[float]:
    """
    Fetch the 5-day percentage return for the sector ETF of the given ticker using yfinance.
    """
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        # Fast info pull to deduce sector
        sector = stock.info.get('sector')
        
        # Default to SPY (S&P 500) if sector mapping is unknown
        etf = SECTOR_ETF_MAP.get(sector, 'SPY') if sector else 'SPY'
        
        etf_ticker = yf.Ticker(etf)
        hist = etf_ticker.history(period=f"{days+2}d") 
        if len(hist) >= 2:
            start_price = float(hist['Close'].iloc[0])
            end_price = float(hist['Close'].iloc[-1])
            return ((end_price - start_price) / start_price) * 100
    except Exception as e:
        print(f"Error fetching sector momentum for {ticker}: {e}")
    return pd.NA


def augment_price_change(
    trades_path: Path = ENRICHED_TRADES_CSV_PATH,
    sleep_sec: float = TIME_SLEEP
) -> pd.DataFrame:
    """
    Load final_trades.csv, fetch current and historical prices, compute metrics,
    add columns 'Current_Price', 'Pct_Change', 'Std_Dev', 'Std_Dev_Away',
    and save back.
    """
    # Load trades, parse Price
    df = pd.read_csv(
        trades_path,
        encoding='utf-8-sig',
        dtype={'Ticker': str, 'Price': str}
    )
    df['Price'] = df['Price'].str.replace(',', '').astype(float)

    # Ensure target columns exist
    for col in ('Current_Price','Pct_Change','Std_Dev','Std_Dev_Away', 'Sector_Momentum'):
        if col not in df.columns:
            df[col] = pd.NA

    # Unique tickers in need of update
    # We update all tickers natively since prices/averages constantly shift
    tickers = df['Ticker'].dropna().unique()

    # Prepare historical price maps
    hist_map: dict[str, list[float]] = {}
    momentum_map: dict[str, Optional[float]] = {}

    # Fetch data per ticker
    print(f"Fetching price data/momentum for {len(tickers)} tickers...")
    for i, t in enumerate(tickers):
        if i % 10 == 0:
            print(f"  [{i}/{len(tickers)}] Ticker: {t}")
        hist = fetch_historical_prices(t, days=5)
        hist_map[t] = hist
        
        mom = fetch_sector_momentum(t, days=5)
        momentum_map[t] = mom
        
        time.sleep(sleep_sec)

    # Compute metrics on DataFrame
    def compute_current_price(row):
        hist = hist_map.get(row['Ticker'], [])
        if hist:
            return sum(hist) / len(hist)
        return row.get('Current_Price', pd.NA)

    def compute_pct(row):
        bought = row['Price']
        curr = row.get('Current_Price')
        if pd.notna(bought) and pd.notna(curr) and bought != 0:
            return (curr - bought) / bought * 100
        return pd.NA

    def compute_std(row):
        bought = row['Price']
        hist = hist_map.get(row['Ticker'], [])
        if pd.notna(bought) and bought != 0 and len(hist) >= 2:
            pct_changes = [(p - bought) / bought * 100 for p in hist]
            return pd.Series(pct_changes).std()
        return pd.NA

    def compute_std_away(row):
        curr = row.get('Current_Price')
        hist = hist_map.get(row['Ticker'], [])
        if pd.notna(curr) and len(hist) >= 2:
            mean_p = sum(hist) / len(hist)
            std_p = pd.Series(hist).std()
            if std_p and std_p > 0:
                return (curr - mean_p) / std_p
        return pd.NA

    df['Current_Price'] = df.apply(compute_current_price, axis=1)
    df['Pct_Change'] = df.apply(compute_pct, axis=1)
    df['Std_Dev'] = df.apply(compute_std, axis=1)
    df['Std_Dev_Away'] = df.apply(compute_std_away, axis=1)
    df['Sector_Momentum'] = df['Ticker'].map(momentum_map)

    # Save updated DataFrame
    df.to_csv(trades_path, index=False, encoding='utf-8-sig')
    return df


if __name__ == '__main__':
    updated = augment_price_change()
    print(f"Updated {len(updated)} rows in {ENRICHED_TRADES_CSV_PATH} with price metrics.")

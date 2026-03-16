"""
alpha_calculator.py

Calculates 1-year rolling Alpha for every congressperson using their
full trade ledger (historical_trades.csv), including both:
  - Closed trades (Buy matched to Sell)
  - Open/unrealized positions (Buy with no corresponding Sell yet)

Methodology:
  For each position an investor holds or held:
    window_start = max(buy_date, 1 year ago)
    window_end   = min(sell_date if sold, today)
    If the position overlapped the last 12 months:
      stock_return = price(window_end) / price(window_start) - 1
      spy_return   = SPY(window_end)  / SPY(window_start)   - 1
      alpha        = stock_return - spy_return

  Average alpha across all positions in window = investor's 1-year Alpha.

Output:
  politician_alpha_metrics.csv  — per-investor aggregated metrics
  active_portfolio_ledger.csv   — all currently open positions
"""
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np

ONE_YEAR_AGO = pd.Timestamp.now().normalize() - pd.DateOffset(years=1)
TODAY        = pd.Timestamp.now().normalize()


def calculate_alpha():
    trades_file = Path(__file__).parent / 'historical_trades.csv'
    if not trades_file.exists():
        print("No historical trades file found. Run historical_scraper.py first.")
        return

    df = pd.read_csv(trades_file, parse_dates=['Traded', 'Published'])
    df = df.sort_values(by='Traded')
    df = df[df['Ticker'].notna() & (df['Ticker'] != '')]

    unique_tickers = df['Ticker'].dropna().unique()

    # ── Fetch SPY benchmark (2y to cover full window_start range) ───────────
    print("Fetching SPY benchmark history...")
    spy_df = yf.Ticker("SPY").history(period="2y")
    if spy_df.empty:
        print("Failed to fetch SPY data.")
        return
    spy_data = spy_df['Close']
    spy_data.index = spy_data.index.tz_localize(None)

    # ── Helper: nearest price on or before target date ─────────────────────
    def get_price(series, target_date):
        if series.empty or pd.isna(target_date):
            return None
        td = pd.Timestamp(target_date)
        past = series[series.index <= td]
        return float(past.iloc[-1]) if not past.empty else None

    # ── Fetch 2y price history for every ticker ─────────────────────────────
    print(f"Fetching 2-year price history for {len(unique_tickers)} tickers...")
    price_histories: dict[str, pd.Series] = {}
    for i, t in enumerate(unique_tickers):
        if i % 100 == 0:
            print(f"  {i}/{len(unique_tickers)} tickers fetched...")
        try:
            hist = yf.Ticker(t).history(period="2y")
            if not hist.empty:
                prices = hist['Close']
                prices.index = prices.index.tz_localize(None)
                price_histories[t] = prices
            else:
                price_histories[t] = pd.Series(dtype=float)
        except Exception:
            price_histories[t] = pd.Series(dtype=float)

    # ── Build portfolio ledger (FIFO Buy→Sell matching) ─────────────────────
    # portfolio[investor][ticker] = [list of open buy_dates]
    portfolio: dict[str, dict[str, list]] = {}
    alpha_records = []

    print("Processing trade ledger (FIFO matching)...")
    for _, row in df.iterrows():
        inv    = row['Investor Name']
        ticker = row['Ticker']
        op     = str(row['Operation']).lower()
        t_date = pd.Timestamp(row['Traded'])

        portfolio.setdefault(inv, {}).setdefault(ticker, [])

        if 'buy' in op:
            portfolio[inv][ticker].append(t_date)

        elif 'sell' in op and portfolio[inv][ticker]:
            buy_date = portfolio[inv][ticker].pop(0)   # FIFO
            sell_date = t_date

            # 1-year window for this closed trade
            window_start = max(buy_date, ONE_YEAR_AGO)
            window_end   = min(sell_date, TODAY)

            if window_start >= window_end:
                continue   # position entirely outside last-year window

            hist = price_histories.get(ticker, pd.Series(dtype=float))
            p_start = get_price(hist,     window_start)
            p_end   = get_price(hist,     window_end)
            s_start = get_price(spy_data, window_start)
            s_end   = get_price(spy_data, window_end)

            if p_start and p_end and p_start > 0 and s_start and s_end:
                stock_ret = (p_end - p_start) / p_start
                spy_ret   = (s_end - s_start) / s_start
                alpha_records.append({
                    'Investor Name': inv,
                    'Ticker':        ticker,
                    'Window_Start':  window_start,
                    'Window_End':    window_end,
                    'Realized':      True,
                    'Stock_Return':  stock_ret,
                    'SPY_Return':    spy_ret,
                    'Alpha':         stock_ret - spy_ret,
                })

    # ── Value all remaining OPEN positions against today ────────────────────
    open_count = 0
    for inv, holdings in portfolio.items():
        for ticker, buy_dates in holdings.items():
            hist = price_histories.get(ticker, pd.Series(dtype=float))
            p_today = get_price(hist, TODAY)
            s_today = get_price(spy_data, TODAY)
            if p_today is None or s_today is None:
                continue

            for buy_date in buy_dates:
                window_start = max(buy_date, ONE_YEAR_AGO)
                window_end   = TODAY

                if window_start >= window_end:
                    continue

                p_start = get_price(hist,     window_start)
                s_start = get_price(spy_data, window_start)

                if p_start and p_start > 0 and s_start:
                    stock_ret = (p_today - p_start) / p_start
                    spy_ret   = (s_today - s_start) / s_start
                    alpha_records.append({
                        'Investor Name': inv,
                        'Ticker':        ticker,
                        'Window_Start':  window_start,
                        'Window_End':    window_end,
                        'Realized':      False,
                        'Stock_Return':  stock_ret,
                        'SPY_Return':    spy_ret,
                        'Alpha':         stock_ret - spy_ret,
                    })
                    open_count += 1

    print(f"Added {open_count} unrealized open-position returns.")

    # ── Aggregate by investor ───────────────────────────────────────────────
    res_df = pd.DataFrame(alpha_records)
    if res_df.empty:
        print("No qualifying positions found in the 1-year window.")
        return

    agg = res_df.groupby('Investor Name').agg(
        Total_Positions     = ('Ticker',       'count'),
        Closed_Positions    = ('Realized',     'sum'),
        Avg_Stock_Return_1Y = ('Stock_Return', 'mean'),
        Avg_SPY_Return_1Y   = ('SPY_Return',   'mean'),
        Average_Alpha_1Y    = ('Alpha',        'mean'),
    ).reset_index()

    # Normalize: clip -50% / +50% → 0.0 / 1.0
    def norm(a):
        return max(-0.5, min(0.5, a)) + 0.5

    agg['Alpha_Score_0_to_1'] = agg['Average_Alpha_1Y'].apply(norm)

    out_path = Path(__file__).parent / 'politician_alpha_metrics.csv'
    agg.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"\nSaved {len(agg)} investor 1-year Alpha metrics to {out_path.name}")
    print(agg[['Investor Name', 'Total_Positions', 'Average_Alpha_1Y', 'Alpha_Score_0_to_1']]
          .sort_values('Average_Alpha_1Y', ascending=False)
          .head(15)
          .to_string(index=False))

    # ── Save active portfolio ledger ────────────────────────────────────────
    active = [
        {'Investor Name': inv, 'Ticker': tkr, 'Buy_Date': d}
        for inv, holdings in portfolio.items()
        for tkr, dates in holdings.items()
        for d in dates
    ]
    if active:
        act_df = pd.DataFrame(active)
        act_path = Path(__file__).parent / 'active_portfolio_ledger.csv'
        act_df.to_csv(act_path, index=False, encoding='utf-8-sig')
        print(f"Saved {len(act_df)} open positions to {act_path.name}")


if __name__ == '__main__':
    calculate_alpha()

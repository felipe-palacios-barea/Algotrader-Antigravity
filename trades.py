
#%%

"""
Module: trades.py

Fetches the latest congressional stock trades from CapitolTrades.com,
parses and normalizes the data, and persists new entries to a CSV.
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

from config import TRADES_CSV_PATH, REQUEST_TIMEOUT


def _parse_date(time_str: str, date_str: str) -> date:
    """
    Parse combined time and date strings into a date.
    - If time_str contains ':', treat date_str as 'Today' or 'Yesterday'.
    - Otherwise, date_str may be 'YYYY' and time_str 'DD Mon'.
    """
    today = datetime.now().date()
    try:
        if ':' in time_str:
            if date_str.lower() == 'today':
                d = today
            elif date_str.lower() == 'yesterday':
                d = today - timedelta(days=1)
            else:
                d = today
            return datetime.fromisoformat(f"{d.isoformat()} {time_str}:00").date()
        # Fallback: full date case, e.g. time_str='14 Jun', date_str='2025'
        return datetime.strptime(f"{time_str} {date_str}", "%d %b %Y").date()
    except Exception:
        return today


def _to_num(s: str) -> Optional[float]:
    """
    Convert shorthand numeric strings with K/M suffix into float.
    E.g. '15K' -> 15000, '2.5M' -> 2500000
    """
    try:
        s_clean = s.strip().upper()
        if s_clean.endswith('K'):
            return float(s_clean[:-1]) * 1_000
        if s_clean.endswith('M'):
            return float(s_clean[:-1]) * 1_000_000
        return float(s_clean)
    except Exception:
        return None


def fetch_latest_trades() -> pd.DataFrame:
    """
    Scrape the latest trades, update the CSV, and return the new rows.
    """
    # 1. Download page
    url = "https://www.capitoltrades.com/trades?pageSize=96"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.select(
        'table.w-full.caption-bottom.text-size-3.text-txt tbody tr.border-b.transition-colors'
    )

    records = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 9:
            continue

        # Investor details
        inv = cols[0]
        name_elem    = inv.select_one('h2.politician-name a')
        party_elem   = inv.select_one('span.party')
        chamber_elem = inv.select_one('span.chamber')
        state_elem   = inv.select_one('span.us-state-compact')

        # Stock details
        stk = cols[1]
        stock_elem  = stk.select_one('h3.issuer-name a')
        ticker_elem = stk.select_one('span.issuer-ticker')

        # Dates
        pub_t_elem = cols[2].select_one('div.text-size-3.font-medium')
        pub_d_elem = cols[2].select_one('div.text-size-2.text-txt-dimmer')
        tr_t_elem  = cols[3].select_one('div.text-size-3.font-medium')
        tr_d_elem  = cols[3].select_one('div.text-size-2.text-txt-dimmer')

        # Parse dates
        published = _parse_date(
            pub_t_elem.get_text(strip=True) if pub_t_elem else '',
            pub_d_elem.get_text(strip=True) if pub_d_elem else ''
        )
        traded = _parse_date(
            tr_t_elem.get_text(strip=True) if tr_t_elem else '',
            tr_d_elem.get_text(strip=True) if tr_d_elem else ''
        )
        filing_time = (published - traded).days

        # Operation
        operation = cols[6].get_text(strip=True)

        # Amount range or single
        amt_txt = cols[7].get_text(strip=True)
        parts  = amt_txt.replace('–', '-').split('-')
        low_amt, high_amt = None, None
        if len(parts) == 2:
            low_amt  = _to_num(parts[0])
            high_amt = _to_num(parts[1])
        else:
            low_amt = _to_num(parts[0])

        # Price
        price = cols[8].get_text(strip=True).replace('$', '')
     

        records.append({
            'Published':        published,
            'Traded':           traded,
            'Filing Time':      filing_time,
            'Investor Name':    name_elem.get_text(strip=True),
            'Party':            party_elem.get_text(strip=True),
            'Chamber':          chamber_elem.get_text(strip=True),
            'State':            state_elem.get_text(strip=True),
            'Stock':            stock_elem.get_text(strip=True),
            'Ticker':           ticker_elem.get_text(strip=True).split(':')[0],
            'Operation':        operation,
            'Low Amount':       low_amt,
            'High Amount':      high_amt,
            'Price':            price,
        })

    new_df = pd.DataFrame(records)

    
    # 2. Load existing
    if TRADES_CSV_PATH.exists():
        existing = pd.read_csv(TRADES_CSV_PATH, encoding='utf-8-sig')
    else:
        existing = pd.DataFrame(columns=new_df.columns)
    


    # 4. Persist combined DataFrame
    updated = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(keep='first')
    updated.to_csv(TRADES_CSV_PATH, index=False, encoding='utf-8-sig')

    hist_path = Path(__file__).parent / 'historical_trades.csv'
    if hist_path.exists() and not new_df.empty:
        hist_df = pd.read_csv(hist_path, encoding='utf-8-sig')
        updated_hist = pd.concat([hist_df, new_df], ignore_index=True).drop_duplicates(
             subset=['Published', 'Traded', 'Investor Name', 'Ticker', 'Operation', 'High Amount'], keep='first')
        updated_hist.to_csv(hist_path, index=False, encoding='utf-8-sig')

    return new_df


if __name__ == '__main__':
    added = fetch_latest_trades()
    print(f"Added {len(added)} new trades") 

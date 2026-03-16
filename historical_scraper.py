import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from pathlib import Path
import time
from typing import Optional

def _parse_date(time_str: str, date_str: str) -> date:
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
        return datetime.strptime(f"{time_str} {date_str}", "%d %b %Y").date()
    except Exception:
        return today

def _to_num(s: str) -> Optional[float]:
    try:
        s_clean = s.strip().upper()
        if s_clean.endswith('K'):
            return float(s_clean[:-1]) * 1_000
        if s_clean.endswith('M'):
            return float(s_clean[:-1]) * 1_000_000
        return float(s_clean)
    except Exception:
        return None

import cloudscraper

def fetch_historical_page(page: int) -> list:
    url = f"https://www.capitoltrades.com/trades?page={page}&pageSize=96"
    print(f"Fetching page {page}...")
    scraper = cloudscraper.create_scraper()
    
    response = None
    for attempt in range(5):
        try:
            response = scraper.get(url, timeout=20)
            response.raise_for_status()
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed to fetch page {page}: {e}")
            time.sleep(3)
            if attempt == 4:
                return []

    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.select('table.w-full.caption-bottom.text-size-3.text-txt tbody tr.border-b.transition-colors')

    records = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 9:
            continue

        inv = cols[0]
        name_elem = inv.select_one('h2.politician-name a')
        if not name_elem:
            continue
        party_elem = inv.select_one('span.party')
        chamber_elem = inv.select_one('span.chamber')
        state_elem = inv.select_one('span.us-state-compact')

        stk = cols[1]
        stock_elem = stk.select_one('h3.issuer-name a')
        ticker_elem = stk.select_one('span.issuer-ticker')
        if not ticker_elem:
            continue

        pub_t_elem = cols[2].select_one('div.text-size-3.font-medium')
        pub_d_elem = cols[2].select_one('div.text-size-2.text-txt-dimmer')
        tr_t_elem = cols[3].select_one('div.text-size-3.font-medium')
        tr_d_elem = cols[3].select_one('div.text-size-2.text-txt-dimmer')

        published = _parse_date(
            pub_t_elem.get_text(strip=True) if pub_t_elem else '',
            pub_d_elem.get_text(strip=True) if pub_d_elem else ''
        )
        traded = _parse_date(
            tr_t_elem.get_text(strip=True) if tr_t_elem else '',
            tr_d_elem.get_text(strip=True) if tr_d_elem else ''
        )
        filing_time = (published - traded).days

        operation = cols[6].get_text(strip=True)

        amt_txt = cols[7].get_text(strip=True)
        parts = amt_txt.replace('–', '-').split('-')
        low_amt, high_amt = None, None
        if len(parts) == 2:
            low_amt = _to_num(parts[0])
            high_amt = _to_num(parts[1])
        else:
            low_amt = _to_num(parts[0])

        price = cols[8].get_text(strip=True).replace('$', '')

        records.append({
            'Published': published,
            'Traded': traded,
            'Filing Time': filing_time,
            'Investor Name': name_elem.get_text(strip=True),
            'Party': party_elem.get_text(strip=True) if party_elem else '',
            'Chamber': chamber_elem.get_text(strip=True) if chamber_elem else '',
            'State': state_elem.get_text(strip=True) if state_elem else '',
            'Stock': stock_elem.get_text(strip=True) if stock_elem else '',
            'Ticker': ticker_elem.get_text(strip=True).split(':')[0],
            'Operation': operation,
            'Low Amount': low_amt,
            'High Amount': high_amt,
            'Price': price,
        })
    return records

def scrape_history(max_pages: int = 150):
    all_records = []
    # Scraping 150 pages gives us around 14,400 historical trades (spanning back a year or longer)
    for i in range(1, max_pages + 1):
        recs = fetch_historical_page(i)
        if not recs:
            print(f"No records found on page {i}. Stopping.")
            break
        all_records.extend(recs)
        time.sleep(0.5)  # Quick pause so CapitolTrades doesn't block us

    df = pd.DataFrame(all_records)
    print(f"Finished scraping {len(df)} total historical trades.")
    
    # Drop duplicates
    if not df.empty:
        df = df.drop_duplicates(subset=['Published', 'Traded', 'Investor Name', 'Ticker', 'Operation', 'High Amount'])
    
    csv_path = Path(__file__).parent / 'historical_trades.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Saved to {csv_path.absolute()}")

if __name__ == '__main__':
    scrape_history(max_pages=150)

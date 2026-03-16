"""
Module: committees.py

Fetches Senate and House committee and subcommittee memberships,
parses committee/subcommittee names, member names, and state codes,
and persists data to a CSV.
Includes fallback for Senate pages without subcommittees.
"""
import re
import pandas as pd
import requests
import cloudscraper
from bs4 import BeautifulSoup
from pathlib import Path

from config import SENATE_COMMITTEE_URLS, HOUSE_COMMITTEE_URLS

# CSV path for combined committee data
COMMITTEES_CSV_PATH = Path(__file__).parent / 'committees.csv'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def fetch_committees() -> pd.DataFrame:
    """
    Scrape Senate and House committee pages,
    normalize fields, update CSV, and return new rows.
    Columns: ['Committee','Subcommittee','Full_Name','State']
    """
    records = []
    scraper = cloudscraper.create_scraper()

    # --- Senate logic with fallback ---
    for url in SENATE_COMMITTEE_URLS:
        try:
            resp = scraper.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"Warning: Failed to fetch {url}: {e}")
            continue
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Base committee name
        h1 = soup.select_one('span.contenttitle')
        if h1 and h1.get_text(strip=True):
            committee_name = h1.get_text(strip=True).replace(',', '.')
        else:
            committee_name = url.rstrip('/').split('/')[-1].split('.')[0].replace(',', '.')

        # Attempt subcommittee extraction
        subtitles = soup.select('span.contentsubtitle subcommittee_name')
        tables = soup.select('table.contenttext')
        if subtitles and tables:
            # Normal subcommittee loop
            for subtitle, table in zip(subtitles, tables):
                sub_name = subtitle.get_text(strip=True).replace(',', '.')
                for row in table.select('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) < 2:
                        continue
                    for cell in (cells[0], cells[1]):
                        for raw in cell.stripped_strings:
                            raw = raw.strip().strip('"')
                            if not raw or raw.lower().startswith('no '):
                                continue
                            m = re.search(r"\(([^)]+)\)", raw)
                            state = m.group(1) if m else 'N/A'
                            name_part = re.sub(r"\s*\([^)]*\)", '', raw).strip()
                            name_part = name_part.strip(' ",')
                            comma_idx = name_part.find(',')
                            paren_idx = raw.find('(')
                            if comma_idx != -1 and paren_idx != -1 and paren_idx < comma_idx:
                                parts = [p.strip() for p in name_part.split(',', 1)]
                                first, last = parts[0], parts[1]
                                full_name = f"{first} {last}"
                            elif comma_idx != -1:
                                parts = [p.strip() for p in name_part.split(',', 1)]
                                last, first = parts[0], parts[1]
                                full_name = f"{first} {last}"
                            else:
                                full_name = name_part
                            records.append({
                                'Committee':    committee_name,
                                'Subcommittee': sub_name,
                                'Full_Name':    full_name,
                                'State':        state
                            })
        else:
            # Fallback: treat entire page as one committee
            table = soup.select_one('table.contenttext')
            if table:
                for row in table.select('tr')[1:]:
                    cells = row.find_all('td')
                    # fallback cell may be first column
                    if not cells:
                        continue
                    cell = cells[0]
                    for raw in cell.stripped_strings:
                        raw = raw.strip().strip('"')
                        if not raw or raw.lower().startswith('no '):
                            continue
                        m = re.search(r"\(([^)]+)\)", raw)
                        state = m.group(1) if m else 'N/A'
                        name_part = re.sub(r"\s*\([^)]*\)", '', raw).strip()
                        name_part = name_part.strip(' ",')
                        comma_idx = name_part.find(',')
                        if comma_idx != -1:
                            last, first = [p.strip() for p in name_part.split(',', 1)]
                            full_name = f"{first} {last}"
                        else:
                            full_name = name_part
                        records.append({
                            'Committee':    committee_name,
                            'Subcommittee': 'N/A',
                            'Full_Name':    full_name,
                            'State':        state
                        })

    # --- House logic ---
    for url in HOUSE_COMMITTEE_URLS:
        sid = url.rstrip('/').split('/')[-1]
        if sid.endswith('00'):
            continue
        try:
            resp = scraper.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"Warning: Failed to fetch {url}: {e}")
            continue
        soup = BeautifulSoup(resp.text, 'html.parser')

        crumb = soup.select_one(
            'ol.breadcrumb.hidden-print li.breadcrumb-item:nth-child(2) a'
        )
        committee = crumb.get_text(strip=True).replace(',', '.') if crumb else 'N/A'

        sub_h1 = soup.select_one('section#accordion h1')
        subcommittee = sub_h1.get_text(strip=True).replace(',', '.') if sub_h1 else sid

        for list_id in ('majority-members', 'minority-members'):
            ul = soup.select_one(f'ul#{list_id}')
            if not ul:
                continue
            for li in ul.select('li.row'):
                link = li.select_one('a.library-link') or li.select_one('a')
                name_span = link.select_one('span') if link else None
                name = name_span.get_text(strip=True) if name_span else 'N/A'
                hidden = li.select_one('span.name[hidden]')
                if hidden:
                    parts = hidden.get_text(strip=True).split()
                    state = parts[-1] if parts else 'N/A'
                else:
                    state = 'N/A'
                records.append({
                    'Committee':    committee,
                    'Subcommittee': subcommittee,
                    'Full_Name':    name,
                    'State':        state
                })

    # Build DataFrame
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df.fillna('N/A', inplace=True)
    df.replace('', 'N/A', inplace=True)
    df = df.drop_duplicates().reset_index(drop=True)

    # Persist
    try:
        existing = pd.read_csv(COMMITTEES_CSV_PATH, encoding='utf-8-sig')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        existing = pd.DataFrame(columns=df.columns)
    combined = pd.concat([existing, df], ignore_index=True)
    combined.fillna('N/A', inplace=True)
    combined.replace('', 'N/A', inplace=True)
    updated = combined.drop_duplicates().reset_index(drop=True)
    updated.to_csv(COMMITTEES_CSV_PATH, index=False, encoding='utf-8-sig')

    return updated


if __name__ == '__main__':
    added = fetch_committees()
    print(f"Added {len(added)} new committee entries")

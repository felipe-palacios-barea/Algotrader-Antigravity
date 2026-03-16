"""
Module: committees.py

Fetches House subcommittee memberships,
parses committee and subcommittee names, member names, and state codes,
and persists data to a CSV.
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path

from config import HOUSE_COMMITTEE_URLS

# CSV path for house subcommittee data
COMMITTEES_CSV_PATH = Path(__file__).parent / 'committees.csv'


def fetch_committees() -> pd.DataFrame:
    """
    Scrape House subcommittee pages, normalize fields,
    update CSV, and return new rows.
    Columns: ['Committee','Subcommittee','Full_Name','State']
    """
    records = []

    for url in HOUSE_COMMITTEE_URLS:
        # Skip main committee pages (ID ending '00')
        sid = url.rstrip('/').split('/')[-1]
        if sid.endswith('00'):
            continue

        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract parent committee from breadcrumb
        crumb = soup.select_one(
            'ol.breadcrumb.hidden-print li.breadcrumb-item:nth-child(2) a'
        )
        committee = crumb.get_text(strip=True) if crumb else 'N/A'
        committee = committee.replace(',', '.')

        # Extract subcommittee title
        sub_h1 = soup.select_one('section#accordion h1')
        subcommittee = sub_h1.get_text(strip=True) if sub_h1 else sid
        subcommittee = subcommittee.replace(',', '.')

        # Process majority & minority members
        for list_id in ('majority-members', 'minority-members'):
            ul = soup.select_one(f'ul#{list_id}')
            if not ul:
                continue
            for li in ul.select('li.row'):
                # Visible name in <a><span>
                link = li.select_one('a.library-link') or li.select_one('a')
                name_span = link.select_one('span') if link else None
                name = name_span.get_text(strip=True) if name_span else 'N/A'

                # Hidden span.name contains "LAST,FIRST ST"
                hidden = li.select_one('span.name[hidden]')
                if hidden:
                    # e.g. "FINSTAD,BRAD MN"
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

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Normalize and dedupe
    df.fillna('N/A', inplace=True)
    df.replace('', 'N/A', inplace=True)
    df = df.drop_duplicates().reset_index(drop=True)

    # Persist to CSV
    try:
        existing = pd.read_csv(COMMITTEES_CSV_PATH, encoding='utf-8-sig')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        existing = pd.DataFrame(columns=df.columns)

    combined = pd.concat([existing, df], ignore_index=True)
    combined.fillna('N/A', inplace=True)
    combined.replace('', 'N/A', inplace=True)
    updated = combined.drop_duplicates().reset_index(drop=True)
    updated.to_csv(COMMITTEES_CSV_PATH, index=False, encoding='utf-8-sig')

    # Return only new rows
    old = set(map(tuple, existing.to_records(index=False)))
    new_rows = updated.loc[~updated.apply(lambda r: tuple(r), axis=1).isin(old)]
    return new_rows


if __name__ == '__main__':
    added = fetch_committees()
    print(f"Added {len(added)} new subcommittee entries")

"""
Module: hearing_scraper.py

Scrapes Congress.gov for recent and upcoming committee hearing dates.
Stores data in hearings.csv for integration with the trading pipeline.
"""

import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path
import re
import time

HEARINGS_CSV_PATH = Path("hearings.csv")

def scrape_congress_hearings():
    """
    Scrapes the hearings search page on Congress.gov.
    Target: https://www.congress.gov/committee-hearings/
    """
    # Using search results endpoint which is often more stable for scraping
    url = "https://www.congress.gov/search?q=%7B%22source%22%3A%22committee-hearings%22%7D"
    scraper = cloudscraper.create_scraper()
    
    print(f"Fetching hearings from {url}...")
    try:
        response = scraper.get(url, timeout=15)
        # 404 on search often means the JSON query wasn't liked, fall back to main list
        if response.status_code == 404:
            url = "https://www.congress.gov/committee-hearings/"
            response = scraper.get(url, timeout=15)
            
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching hearings: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Each hearing is typically in an <li> or predictable structure
    # On Congress.gov, search results are often in <li class="expanded">
    hearing_items = soup.select('li.expanded')
    
    records = []
    for item in hearing_items:
        # Title/Topic
        title_elem = item.select_one('span.result-heading a')
        title = title_elem.get_text(strip=True) if title_elem else "N/A"
        
        # Meta info (Date, Committee)
        meta_text = item.get_text(separator='|', strip=True)
        
        # Extract Date (e.g., March 14, 2026)
        date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', meta_text)
        hearing_date = date_match.group(1) if date_match else "N/A"
        
        # Extract Committee
        committee_match = re.search(r'Committee on ([^|]+)', meta_text)
        committee = committee_match.group(1).strip() if committee_match else "N/A"
        
        # Subcommittee if present
        subcommittee = "N/A"
        sub_match = re.search(r'Subcommittee on ([^|]+)', meta_text)
        if sub_match:
            subcommittee = sub_match.group(1).strip()

        records.append({
            'Hearing_Title': title,
            'Hearing_Date': hearing_date,
            'Committee': committee,
            'Subcommittee': subcommittee
        })

    df = pd.DataFrame(records)
    if not df.empty:
        # Convert date to datetime objects for easier comparison later
        df['Date_Parsed'] = pd.to_datetime(df['Hearing_Date'], errors='coerce')
        df = df.dropna(subset=['Date_Parsed'])
        
        # Save or Update
        if HEARINGS_CSV_PATH.exists():
            existing = pd.read_csv(HEARINGS_CSV_PATH)
            df = pd.concat([existing, df]).drop_duplicates(subset=['Hearing_Title', 'Hearing_Date'])
            
        df.to_csv(HEARINGS_CSV_PATH, index=False)
        print(f"Stored {len(df)} unique hearings in {HEARINGS_CSV_PATH}")
    else:
        print("No hearings found.")

    return df

if __name__ == "__main__":
    scrape_congress_hearings()

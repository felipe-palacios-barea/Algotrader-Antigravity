"""
Direct live test bypassing the run-log cache.
Tests the actual fetch logic of both fixed sources.
"""
import requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import re

API_KEY = "CNfblf0YqrIaYTGvwHHgbzyXgwPKb55PvoGMMAKM"
BASE = "https://api.congress.gov/v3"
FETCH_DAYS = 90
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CongressResearchBot/1.0; research)"}

print("=" * 65)
print("DIRECT API TESTS (no cache)")
print("=" * 65)

# --- TEST 1: Congress.gov sponsored legislation (Boozman) ---
print("\n[1] Congress.gov Sponsored Legislation — Boozman (B001236)")
r = requests.get(
    BASE + "/member/B001236/sponsored-legislation",
    params={"api_key": API_KEY, "limit": 5, "format": "json"},
    timeout=15
)
bills = r.json().get("sponsoredLegislation", [])
print(f"  Bills returned: {len(bills)}")
for bill in bills[:3]:
    title = bill.get("title", "")
    latest = bill.get("latestAction") or {}
    action = latest.get("text", "") if isinstance(latest, dict) else ""
    intro  = bill.get("introducedDate", "")
    print(f"  Title:  {title[:70]}")
    print(f"  Action: {action[:70]}")
    print(f"  Date:   {intro}")
    print()

# --- TEST 2: Congress.gov sponsored legislation (Ted Cruz) ---
print("[2] Congress.gov Sponsored Legislation — Ted Cruz (C001098)")
r = requests.get(
    BASE + "/member/C001098/sponsored-legislation",
    params={"api_key": API_KEY, "limit": 5, "format": "json"},
    timeout=15
)
bills = r.json().get("sponsoredLegislation", [])
print(f"  Bills returned: {len(bills)}")
for bill in bills[:3]:
    title = bill.get("title", "")
    latest = bill.get("latestAction") or {}
    action = latest.get("text", "") if isinstance(latest, dict) else ""
    intro  = bill.get("introducedDate", "")
    print(f"  Title:  {title[:70]}")
    print(f"  Action: {action[:70]}")
    print(f"  Date:   {intro}")
    print()

# --- TEST 3: RSS feeds for a few senators ---
print("[3] RSS Feeds — Testing multiple senators")
test_slugs = [
    ("John Boozman",    "boozman",  "https://www.boozman.senate.gov/public/?p=PressReleases&ContentType=application/rss+xml"),
    ("Ted Cruz",        "cruz",     "https://www.cruz.senate.gov/rss/feeds/?type=press-releases"),
    ("Elizabeth Warren","warren",   "https://www.warren.senate.gov/newsroom/press-releases/feed"),
    ("Lindsey Graham",  "lgraham",  "https://www.lgraham.senate.gov/public/?p=PressReleases&ContentType=application/rss+xml"),
    ("Chuck Grassley",  "grassley", "https://www.grassley.senate.gov/public/?p=PressReleases&ContentType=application/rss+xml"),
    ("Marco Rubio",     "rubio",    "https://www.rubio.senate.gov/public/?p=PressReleases&ContentType=application/rss+xml"),
]

for name, slug, rss_url in test_slugs:
    try:
        resp = requests.get(rss_url, headers=HEADERS, timeout=10)
        ct = resp.headers.get("content-type", "")
        ok = resp.status_code == 200 and ("xml" in ct or "rss" in ct or resp.text.strip().startswith("<"))
        if ok:
            try:
                soup = BeautifulSoup(resp.text, "lxml-xml")
            except Exception:
                soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("item") or soup.find_all("entry")
            # Sample first item
            if items:
                t = items[0].find("title")
                d = items[0].find("pubDate") or items[0].find("published")
                print(f"  OK  {name:<22} items={len(items):<4} latest: {t.get_text()[:50] if t else '?'}")
            else:
                print(f"  OK  {name:<22} but 0 items parsed (may be HTML not RSS)")
        else:
            print(f"  FAIL {name:<22} status={resp.status_code} ct={ct[:40]}")
    except Exception as e:
        print(f"  ERR  {name:<22} {str(e)[:60]}")

print("\n" + "=" * 65)
print("DONE")
print("=" * 65)

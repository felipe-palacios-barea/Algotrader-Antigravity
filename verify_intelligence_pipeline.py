# -*- coding: utf-8 -*-
"""Verification script for the Political Intelligence Pipeline."""
import sys
import os
import json
import requests
from datetime import datetime, timedelta

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from political_data_store import PoliticalDataStore

API_KEY = "CNfblf0YqrIaYTGvwHHgbzyXgwPKb55PvoGMMAKM"
BASE = "https://api.congress.gov/v3"

print("=" * 60)
print("VERIFICATION RUN")
print("=" * 60)

# --- Test 1: API connectivity --------------------------------------------------
print("\n[Test 1] Congress.gov API connectivity...")
try:
    r = requests.get(BASE + "/member", params={"api_key": API_KEY, "limit": 5, "format": "json"}, timeout=15)
    r.raise_for_status()
    members = r.json().get("members", [])
    print(f"  [OK] {len(members)} members returned in test call")
    for m in members[:3]:
        print(f"       - {m.get('name', '?')}")
except Exception as e:
    print(f"  [FAIL] {e}")

# --- Test 2: House communications ----------------------------------------------
print("\n[Test 2] House communications (last 30 days)...")
cutoff = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
try:
    r = requests.get(
        BASE + "/house-communication",
        params={"api_key": API_KEY, "fromDateTime": cutoff + "T00:00:00Z", "limit": 6, "format": "json"},
        timeout=15,
    )
    r.raise_for_status()
    comms = r.json().get("houseCommunications", [])
    print(f"  [OK] {len(comms)} House communications found")
    for i, c in enumerate(comms[:5]):
        ctype = c.get("communicationType", {}).get("name", "?")
        subj = c.get("subject", "N/A")[:50]
        print(f"       {i+1}. Type={ctype!r:<25} Subject={subj}")
except Exception as e:
    print(f"  [FAIL] {e}")

# --- Test 3: Senate communications --------------------------------------------
print("\n[Test 3] Senate communications (last 30 days)...")
try:
    r = requests.get(
        BASE + "/senate-communication",
        params={"api_key": API_KEY, "fromDateTime": cutoff + "T00:00:00Z", "limit": 6, "format": "json"},
        timeout=15,
    )
    r.raise_for_status()
    comms = r.json().get("senateCommunications", [])
    print(f"  [OK] {len(comms)} Senate communications found")
    for i, c in enumerate(comms[:5]):
        ctype = c.get("communicationType", {}).get("name", "?")
        subj = c.get("subject", "N/A")[:50]
        print(f"       {i+1}. Type={ctype!r:<25} Subject={subj}")
except Exception as e:
    print(f"  [FAIL] {e}")

# --- Test 4: Member profile ---------------------------------------------------
print("\n[Test 4] Member profile - John Boozman (B001236)...")
try:
    r = requests.get(BASE + "/member/B001236", params={"api_key": API_KEY, "format": "json"}, timeout=15)
    r.raise_for_status()
    m = r.json().get("member", {})
    name = m.get("directOrderName", "N/A")
    state = m.get("state", "?")
    terms = m.get("terms", {})
    terms_list = terms.get("item", []) if isinstance(terms, dict) else terms
    print(f"  [OK] {name} / {state} / {len(terms_list)} terms in Congress")
except Exception as e:
    print(f"  [FAIL] {e}")

# --- Test 5: DB insert + dedup + query ----------------------------------------
print("\n[Test 5] Database insert / dedup / query...")
try:
    store = PoliticalDataStore()

    r1 = store.insert_signal(
        "congress_gov", "John Boozman", datetime.utcnow() - timedelta(days=3),
        "Support for American Grain Exporters",
        "Senator Boozman spoke in favor of agricultural export subsidies citing Bunge and Archer Daniels Midland.",
        "https://congress.gov/verify/test1",
    )
    r2 = store.insert_signal(
        "press_release", "Ted Cruz", datetime.utcnow() - timedelta(days=1),
        "Cruz Calls for Big Tech Investigation",
        "Senator Cruz called for antitrust probes into Google and Amazon targeting advertising algorithms.",
        "https://cruz.senate.gov/verify/test1",
    )
    r_dup = store.insert_signal(
        "congress_gov", "John Boozman", datetime.utcnow(),
        "Duplicate - should be blocked", "Duplicate content.",
        "https://congress.gov/verify/test1",  # Same URL -> must be blocked
    )

    print(f"  Insert 1 (Boozman press):  {'NEW record inserted' if r1 else 'Already existed (skip)'}")
    print(f"  Insert 2 (Cruz release):   {'NEW record inserted' if r2 else 'Already existed (skip)'}")
    print(f"  Insert 3 (duplicate):      {'ERROR: Was inserted' if r_dup else 'BLOCKED correctly (dedup works)'}")

    stats = store.get_stats()
    print(f"\n  DB Stats:")
    print(f"    Total signals:   {stats['total_signals']}")
    print(f"    Enriched:        {stats['enriched']}")
    print(f"    Pending GPT:     {stats['pending_enrichment']}")
    print(f"    By source:       {stats['by_source']}")

    print("\n  All DB records (last 30 days):")
    print(f"  {'Source':<18} {'Member':<22} {'Title'}")
    print(f"  {'-'*18} {'-'*22} {'-'*45}")
    for row in store.query_intelligence(days_back=30):
        print(f"  {row['source']:<18} {row['member_name']:<22} {row['title'][:45]}")

except Exception as e:
    print(f"  [FAIL] {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)

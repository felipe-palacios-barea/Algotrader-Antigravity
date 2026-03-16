"""
Quick live test of the fixed fetchers against 3 members.
Tests: CongressGov sponsored-legislation + RSS press releases.
"""
import sys
from political_intelligence_fetcher import (
    CongressGovSource, RSSPressReleaseSource, load_members
)
from political_data_store import PoliticalDataStore

store = PoliticalDataStore()
all_members = load_members()

# Test on 3 diverse members: one Senator (R), one Senator (D), one House member
test_names = ["John Boozman", "Ted Cruz", "Elizabeth Warren"]
test_members = [m for m in all_members if m["member_name"] in test_names]

if not test_members:
    test_members = all_members[:3]

print("=" * 65)
print("FIXED FETCHER LIVE TEST")
print("=" * 65)

# --- Test 1: Congress.gov sponsored legislation ---
print("\n[Congress.gov Sponsored Legislation]")
cg = CongressGovSource(store)
for member in test_members:
    print(f"\n  Member: {member['member_name']} ({member['congress_bioguide_id']})")
    signals = cg.fetch(member)
    print(f"  Signals returned: {len(signals)}")
    for s in signals[:3]:
        print(f"    Title:   {s['title'][:70]}")
        print(f"    Content: {s['content_raw'][:100]}")
        print(f"    Date:    {s.get('posted_at')}")
        print()

# --- Test 2: RSS press releases ---
print("\n[RSS Press Release Feeds]")
rss = RSSPressReleaseSource(store)
for member in test_members:
    print(f"\n  Member: {member['member_name']}")
    signals = rss.fetch(member)
    print(f"  Signals returned: {len(signals)}")
    for s in signals[:3]:
        print(f"    Title:   {s['title'][:70]}")
        print(f"    Content: {s['content_raw'][:120]}")
        print(f"    Date:    {s.get('posted_at')}")
        print()

print("=" * 65)
print("TEST COMPLETE")
print("=" * 65)

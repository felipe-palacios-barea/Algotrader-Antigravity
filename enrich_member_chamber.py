"""
Enrich member_handles.csv with a 'chamber' column (Senate/House)
by querying the Congress.gov member detail endpoint.
Run once only.
"""
import requests
import pandas as pd
import time

API_KEY = "CNfblf0YqrIaYTGvwHHgbzyXgwPKb55PvoGMMAKM"
BASE = "https://api.congress.gov/v3"

df = pd.read_csv("member_handles.csv", dtype=str, keep_default_na=False)

if "chamber" in df.columns and df["chamber"].ne("").all():
    print("Chamber column already populated. Done.")
else:
    chambers = []
    for _, row in df.iterrows():
        bio = row.get("congress_bioguide_id", "").strip()
        if not bio:
            chambers.append("")
            continue
        try:
            r = requests.get(
                f"{BASE}/member/{bio}",
                params={"api_key": API_KEY, "format": "json"},
                timeout=15,
            )
            if r.status_code == 200:
                member = r.json().get("member", {})
                terms = member.get("terms", {})
                # terms is a list or dict depending on API version
                if isinstance(terms, dict):
                    items = terms.get("item", [])
                elif isinstance(terms, list):
                    items = terms
                else:
                    items = []
                
                chamber = ""
                if items:
                    # Most recent term first
                    last = items[-1] if isinstance(items, list) else items
                    chamber = last.get("chamber", "")
                chambers.append(chamber)
                print(f"  {row['member_name']:<25} -> {chamber}")
            else:
                print(f"  {row['member_name']:<25} -> API error {r.status_code}")
                chambers.append("")
        except Exception as e:
            print(f"  {row['member_name']:<25} -> ERROR {e}")
            chambers.append("")
        time.sleep(0.3)

    df["chamber"] = chambers
    df.to_csv("member_handles.csv", index=False)
    print(f"\nSaved. Unique chambers: {df['chamber'].unique().tolist()}")

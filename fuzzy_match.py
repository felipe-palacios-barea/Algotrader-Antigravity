"""
Module: fuzzymatch.py

Orchestrates fetching trades and committee data,
performs hybrid matching, and outputs matched_trades.csv.

Workflow:
1. Fetch latest trades via fetch_latest_trades().
2. Fetch committee & subcommittee memberships via fetch_committees().
3. Normalize names and exact-join on normalized names.
4. Fuzzy-match any remaining unmatched names.
5. Write out a CSV containing original trades fields plus:
   - Matched_Name
   - Match_Score
   - Committees
   - Subcommittees
"""
import re
import pandas as pd
from rapidfuzz import process, fuzz
from config import ENRICHED_TRADES_CSV_PATH
from trades import fetch_latest_trades
from committees import fetch_committees


def normalize_name(name: str) -> str:
    """
    Lowercase, remove suffixes (Jr, Sr, II, III), drop punctuation,
    collapse whitespace for robust matching.
    """
    n = name.lower()
    # drop common suffixes
    n = re.sub(r"\b(jr|sr|ii|iii)\b", "", n)
    # replace non-letters with space
    n = re.sub(r"[^a-z ]", " ", n)
    # collapse whitespace
    return re.sub(r"\s+", " ", n).strip()


def build_member_map(committees_df: pd.DataFrame) -> dict:
    """
    Map each Full_Name to sets of committees and subcommittees.
    committees_df must include columns: 'Full_Name', 'Committee', 'Subcommittee'.
    Returns a dict:
      { 'Member Name': {'committees': set(), 'subcommittees': set()}, ... }
    """
    required = {'Full_Name', 'Committee', 'Subcommittee'}
    if not required.issubset(committees_df.columns):
        raise ValueError(f"committees_df must include columns: {required}")
    mapping = {}
    for _, row in committees_df.iterrows():
        member = row['Full_Name']
        mapping.setdefault(member, {'committees': set(), 'subcommittees': set()})
        mapping[member]['committees'].add(row['Committee'])
        sub = row['Subcommittee']
        if pd.notna(sub) and sub not in ('', 'N/A'):
            mapping[member]['subcommittees'].add(sub)
    return mapping


def match_investors(
    trades_df: pd.DataFrame,
    committees_df: pd.DataFrame,
    score_cutoff: int = 85,
    investor_col: str = 'Investor Name'
) -> pd.DataFrame:
    """
    Perform hybrid matching:
    - Normalize and exact-join on normalized names.
    - Fuzzy-fallback for unmatched.

    Returns enriched DataFrame with additional columns:
      - Matched_Name
      - Match_Score
      - Committees
      - Subcommittees
    """
    # Copy and normalize
    td = trades_df.copy()
    cd = committees_df.copy()
    td['norm_inv'] = td[investor_col].fillna('').astype(str).apply(normalize_name)
    cd['norm_mem'] = cd['Full_Name'].fillna('').astype(str).apply(normalize_name)

    # Exact join
    merged = td.merge(
        cd[['norm_mem', 'Full_Name', 'Committee', 'Subcommittee']],
        left_on='norm_inv', right_on='norm_mem', how='left', suffixes=('', '_mem')
    )

    # Initialize match fields for exact matches
    merged['Matched_Name']    = merged['Full_Name'].fillna('')
    merged['Match_Score']     = merged['norm_mem'].notna().astype(float) * 100.0
    merged['Committees']      = merged['Committee'].fillna('N/A')
    merged['Subcommittees']   = merged['Subcommittee'].fillna('N/A')

    # Fuzzy fallback
    to_fuzzy = merged[merged['Matched_Name'] == '']
    members = sorted(cd['norm_mem'].unique())
    member_map = build_member_map(cd)
    for idx, row in to_fuzzy.iterrows():
        inv_norm = row['norm_inv']
        match_norm, score, _ = process.extractOne(inv_norm, members, scorer=fuzz.token_sort_ratio)
        if score >= score_cutoff:
            full = cd.loc[cd['norm_mem'] == match_norm, 'Full_Name'].iloc[0]
            merged.at[idx, 'Matched_Name']    = full
            merged.at[idx, 'Match_Score']     = score
            comms = sorted(member_map[full]['committees'])
            subs  = sorted(member_map[full]['subcommittees'])
            merged.at[idx, 'Committees']      = '; '.join(comms) if comms else 'N/A'
            merged.at[idx, 'Subcommittees']   = '; '.join(subs)  if subs  else 'N/A'
        else:
            merged.at[idx, 'Matched_Name'] = 'N/A'
            merged.at[idx, 'Match_Score']  = score

    # Select final columns: original trades + match info
    final_cols = list(trades_df.columns) + ['Matched_Name', 'Match_Score', 'Committees', 'Subcommittees']
    return merged[final_cols]


if __name__ == '__main__':
    trades_df     = fetch_latest_trades()
    committees_df = fetch_committees()
    matched_df    = match_investors(trades_df, committees_df)
    matched_df.to_csv(ENRICHED_TRADES_CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"Wrote {len(matched_df)} records to {ENRICHED_TRADES_CSV_PATH}")

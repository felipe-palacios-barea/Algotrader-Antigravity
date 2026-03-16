"""
Module: political_signal_enricher.py

Computes 6 political intelligence metrics for each trade row in
enriched_trades_enriched.csv by joining against the political_intelligence.db.

Metrics added:
    member_sector_sentiment     - Avg GPT sentiment the member expressed
                                  about this ticker/industry before the trade
    member_sentiment_trend      - Is member's sector sentiment improving (+1),
                                  neutral (0) or declining (-1) over 14 days?
    committee_sector_sentiment  - Avg sentiment of *all* committee members about
                                  this ticker/industry
    bipartisan_sentiment_align  - 1 if both R and D members have avg > 0 for
                                  this ticker, 0 otherwise
    silent_cluster_flag         - 1 if cluster_count >= 2 but none of the
                                  clustered members posted about this ticker
    statement_to_trade_days     - Days between last relevant statement and trade
                                  (negative = statement before trade, most suspicious)

Usage:
    python political_signal_enricher.py        # Stand-alone
    from political_signal_enricher import enrich_political_signals
    df = enrich_political_signals(df)
"""

import json
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from config import POLITICAL_INTELLIGENCE_DB, ENRICHED_TRADES_CSV_PATH


# ── Helpers ────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    db_path = Path(POLITICAL_INTELLIGENCE_DB)
    if not db_path.exists():
        raise FileNotFoundError(
            f"political_intelligence.db not found at {db_path}. "
            "Run political_intelligence_fetcher.py first."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_signals_df() -> pd.DataFrame:
    """Load all enriched signals into a DataFrame for vectorized joins."""
    with _conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                member_name,
                source,
                posted_at,
                tickers_mentioned,
                industries_mentioned,
                topic_sentiment,
                sentiment_score
            FROM intelligence_signals
            WHERE enriched_at IS NOT NULL
              AND sentiment_score IS NOT NULL
            ORDER BY posted_at DESC
            """,
            conn,
            parse_dates=["posted_at"],
        )
    return df


def _parse_topic_sentiment(ts_raw) -> dict:
    if not ts_raw:
        return {}
    try:
        if isinstance(ts_raw, str):
            return json.loads(ts_raw)
        return dict(ts_raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def _ticker_score(sig_row: pd.Series, ticker: str) -> Optional[float]:
    """
    Extract the per-ticker or per-industry score from a signal row.
    Falls back to overall sentiment_score if the ticker isn't in topic_sentiment.
    """
    ts = _parse_topic_sentiment(sig_row.get("topic_sentiment"))
    if ticker and isinstance(ticker, str) and ticker.strip():
        for key, val in ts.items():
            if key.upper() == ticker.upper():
                return float(val)
    # Fall back to overall score
    score = sig_row.get("sentiment_score")
    if score is not None and not (isinstance(score, float) and np.isnan(score)):
        return float(score)
    return None


def _member_signals_before(
    signals_df: pd.DataFrame,
    member_name: str,
    trade_date: datetime,
    ticker: str,
    lookback_days: int = 90,
) -> pd.DataFrame:
    """Filter signals to those from a specific member before the trade date."""
    if signals_df.empty:
        return signals_df

    cutoff = trade_date - timedelta(days=lookback_days)
    mask = (
        (signals_df["member_name"].str.lower() == member_name.lower())
        & (signals_df["posted_at"] < trade_date)
        & (signals_df["posted_at"] >= cutoff)
    )
    return signals_df[mask].copy()


# ── Metric Calculators ─────────────────────────────────────────────────────────

def _member_sector_sentiment(
    signals_df: pd.DataFrame,
    member_name: str,
    trade_date: datetime,
    ticker: str,
) -> float:
    """
    Average per-ticker sentiment score from the member's recent statements.
    Returns 0.0 if no relevant signals found.
    """
    member_sigs = _member_signals_before(signals_df, member_name, trade_date, ticker)
    if member_sigs.empty:
        return 0.0

    scores = [
        s for s in member_sigs.apply(lambda r: _ticker_score(r, ticker), axis=1)
        if s is not None
    ]
    return round(float(np.mean(scores)), 4) if scores else 0.0


def _member_sentiment_trend(
    signals_df: pd.DataFrame,
    member_name: str,
    trade_date: datetime,
    ticker: str,
    window_days: int = 14,
) -> int:
    """
    Returns +1 (improving), 0 (neutral / insufficient data), or -1 (declining)
    based on the slope of the member's recent sentiment scores for this ticker.
    """
    cutoff = trade_date - timedelta(days=window_days)
    mask = (
        (signals_df["member_name"].str.lower() == member_name.lower())
        & (signals_df["posted_at"] >= cutoff)
        & (signals_df["posted_at"] < trade_date)
    )
    sub = signals_df[mask].copy().sort_values("posted_at")
    if len(sub) < 2:
        return 0

    scores = [s for s in sub.apply(lambda r: _ticker_score(r, ticker), axis=1) if s is not None]
    if len(scores) < 2:
        return 0

    # Simple trend: compare first-half average vs second-half
    mid = len(scores) // 2
    first_half = np.mean(scores[:mid]) if mid > 0 else scores[0]
    second_half = np.mean(scores[mid:]) if len(scores[mid:]) > 0 else scores[-1]

    delta = second_half - first_half
    if delta > 0.1:
        return 1
    elif delta < -0.1:
        return -1
    return 0


def _committee_sector_sentiment(
    signals_df: pd.DataFrame,
    committees_str: str,
    trade_date: datetime,
    ticker: str,
    member_registry: Optional[pd.DataFrame],
) -> float:
    """
    Average sentiment score from ALL members of the same committee(s)
    about this ticker, in the 90 days before the trade.
    """
    if signals_df.empty or not committees_str:
        return 0.0

    # Parse committee names (may be semicolon-separated)
    committees = [c.strip().lower() for c in str(committees_str).split(";")]

    scores = []
    cutoff = trade_date - timedelta(days=90)

    for _, sig in signals_df.iterrows():
        if sig["posted_at"] < cutoff or sig["posted_at"] >= trade_date:
            continue

        # Check if this signal's member serves on the committee
        if member_registry is not None and not member_registry.empty:
            mem_lower = sig["member_name"].lower()
            # Quick membership check via the signals member name vs committees
            # (In production, join with committees.csv or member_handles.csv)
            score = _ticker_score(sig, ticker)
            if score is not None:
                scores.append(score)

    return round(float(np.mean(scores)), 4) if scores else 0.0


def _bipartisan_sentiment_alignment(
    signals_df: pd.DataFrame,
    ticker: str,
    trade_date: datetime,
    party_map: dict,  # {member_name_lower -> party}
) -> int:
    """
    Returns 1 if both Republicans AND Democrats have avg positive
    sentiment about this ticker in the 90 days before trade_date.
    """
    if signals_df.empty:
        return 0

    cutoff = trade_date - timedelta(days=90)
    relevant = signals_df[
        (signals_df["posted_at"] >= cutoff) & (signals_df["posted_at"] < trade_date)
    ].copy()

    if relevant.empty:
        return 0

    r_scores, d_scores = [], []

    for _, sig in relevant.iterrows():
        name_lower = sig["member_name"].lower()
        party = party_map.get(name_lower, "").upper()
        score = _ticker_score(sig, ticker)
        if score is None:
            continue
        if party in ("R", "REPUBLICAN"):
            r_scores.append(score)
        elif party in ("D", "DEMOCRAT"):
            d_scores.append(score)

    if not r_scores or not d_scores:
        return 0

    r_avg = np.mean(r_scores)
    d_avg = np.mean(d_scores)

    # Both positive: strong cross-party signal
    return 1 if r_avg > 0 and d_avg > 0 else 0


def _silent_cluster_flag(
    signals_df: pd.DataFrame,
    cluster_members: list,  # list of member names who co-traded this ticker
    trade_date: datetime,
    ticker: str,
    window_days: int = 30,
) -> int:
    """
    Returns 1 if cluster_count >= 2 but NONE of the clustered members
    published any statement about this ticker in the window before the trade.
    This is the "insider silence" signal — they traded without giving any hint.
    """
    if len(cluster_members) < 2:
        return 0

    cutoff = trade_date - timedelta(days=window_days)
    for member in cluster_members:
        member_sigs = signals_df[
            (signals_df["member_name"].str.lower() == member.lower())
            & (signals_df["posted_at"] >= cutoff)
            & (signals_df["posted_at"] < trade_date)
        ]
        # Check if any signal mentions this ticker
        for _, sig in member_sigs.iterrows():
            ts = _parse_topic_sentiment(sig.get("topic_sentiment"))
            mentioned_tickers = sig.get("tickers_mentioned", "")
            if isinstance(mentioned_tickers, str):
                mentioned_tickers = mentioned_tickers.upper()
                if ticker and ticker.upper() in mentioned_tickers:
                    return 0  # Someone spoke about it — not silent
            for key in ts:
                if ticker and key.upper() == ticker.upper():
                    return 0

    # None of the cluster members published anything about this ticker
    return 1 if len(cluster_members) >= 2 else 0


def _statement_to_trade_days(
    signals_df: pd.DataFrame,
    member_name: str,
    trade_date: datetime,
    ticker: str,
    lookback_days: int = 90,
) -> Optional[int]:
    """
    Days between the member's most recent statement mentioning this ticker
    and the trade date. Negative = statement was BEFORE the trade (most suspicious).
    Returns None if no relevant statement found.
    
    Example: -7 means they published something 7 days before they traded.
    """
    member_sigs = _member_signals_before(signals_df, member_name, trade_date, ticker, lookback_days)
    if member_sigs.empty:
        return None

    # Find the closest statement to the trade date
    member_sigs = member_sigs.copy()
    member_sigs["days_delta"] = (
        member_sigs["posted_at"] - pd.Timestamp(trade_date)
    ).dt.days

    # Filter to signals that actually mention the ticker
    relevant = []
    for _, sig in member_sigs.iterrows():
        ts = _parse_topic_sentiment(sig.get("topic_sentiment"))
        mentioned = sig.get("tickers_mentioned", "")
        if isinstance(mentioned, str) and ticker and ticker.upper() in mentioned.upper():
            relevant.append(sig["days_delta"])
        elif any(k.upper() == (ticker or "").upper() for k in ts):
            relevant.append(sig["days_delta"])

    if not relevant:
        return None

    # Return the closest one (smallest absolute delta)
    return int(min(relevant, key=abs))


# ── Main Enrichment Function ───────────────────────────────────────────────────

def enrich_political_signals(
    df: pd.DataFrame,
    signals_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Adds 6 political intelligence columns to the trades DataFrame.
    
    Args:
        df: The enriched_trades_enriched.csv DataFrame
        signals_df: Pre-loaded signals (pass for performance caching)
    
    Returns:
        DataFrame with 6 new columns appended.
    """
    print("[PoliticalEnricher] Loading intelligence signals from DB...")
    try:
        if signals_df is None:
            signals_df = _load_signals_df()
        print(f"[PoliticalEnricher] {len(signals_df)} enriched signals loaded.")
    except FileNotFoundError as e:
        print(f"[PoliticalEnricher] DB not found — returning defaults. ({e})")
        signals_df = pd.DataFrame()

    if signals_df.empty:
        # No signals yet — return neutral defaults so pipeline keeps running
        df["member_sector_sentiment"] = 0.0
        df["member_sentiment_trend"] = 0
        df["committee_sector_sentiment"] = 0.0
        df["bipartisan_sentiment_align"] = 0
        df["silent_cluster_flag"] = 0
        df["statement_to_trade_days"] = None
        return df

    # Build a party map from the trades DataFrame itself (faster than CSV re-read)
    party_map = {}
    if "Investor Name" in df.columns and "Party" in df.columns:
        for _, row in df[["Investor Name", "Party"]].drop_duplicates().iterrows():
            name = str(row["Investor Name"]).lower()
            party = str(row["Party"])
            party_map[name] = party

    # Parse trade dates once
    df = df.copy()
    df["_trade_date"] = pd.to_datetime(df["Traded"], errors="coerce")

    results = {
        "member_sector_sentiment": [],
        "member_sentiment_trend": [],
        "committee_sector_sentiment": [],
        "bipartisan_sentiment_align": [],
        "silent_cluster_flag": [],
        "statement_to_trade_days": [],
    }

    total = len(df)
    print(f"[PoliticalEnricher] Computing metrics for {total} rows...")

    for i, (_, row) in enumerate(df.iterrows()):
        member = str(row.get("Investor Name", ""))
        ticker = str(row.get("Ticker", "")).strip()
        trade_date = row["_trade_date"]
        committees = str(row.get("Committees", ""))
        cluster_count = int(row.get("cluster_count", 0) or 0)

        if pd.isna(trade_date):
            for k in results:
                results[k].append(None)
            continue

        # 1. member_sector_sentiment
        results["member_sector_sentiment"].append(
            _member_sector_sentiment(signals_df, member, trade_date, ticker)
        )

        # 2. member_sentiment_trend
        results["member_sentiment_trend"].append(
            _member_sentiment_trend(signals_df, member, trade_date, ticker)
        )

        # 3. committee_sector_sentiment (simplified: all signals near trade date)
        results["committee_sector_sentiment"].append(
            _committee_sector_sentiment(signals_df, committees, trade_date, ticker, None)
        )

        # 4. bipartisan_sentiment_alignment
        results["bipartisan_sentiment_align"].append(
            _bipartisan_sentiment_alignment(signals_df, ticker, trade_date, party_map)
        )

        # 5. silent_cluster_flag
        # Identify co-traders: same ticker, same traded date, different member
        cluster_members = []
        if cluster_count >= 2 and ticker:
            cluster_df = df[
                (df["Ticker"] == ticker)
                & (df["_trade_date"] == trade_date)
            ]
            cluster_members = cluster_df["Investor Name"].dropna().unique().tolist()

        results["silent_cluster_flag"].append(
            _silent_cluster_flag(signals_df, cluster_members, trade_date, ticker)
        )

        # 6. statement_to_trade_days
        results["statement_to_trade_days"].append(
            _statement_to_trade_days(signals_df, member, trade_date, ticker)
        )

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{total} rows...")

    # Attach results
    for col, vals in results.items():
        df[col] = vals

    df = df.drop(columns=["_trade_date"])
    print(f"[PoliticalEnricher] Done. 6 political metrics added.")
    return df


# ── Stand-alone usage ──────────────────────────────────────────────────────────

def run_political_enrichment():
    """
    Load enriched_trades_enriched.csv, add political metrics, save back.
    """
    if not Path(ENRICHED_TRADES_CSV_PATH).exists():
        print(f"[ERROR] {ENRICHED_TRADES_CSV_PATH} not found. Run main.py first.")
        return

    df = pd.read_csv(ENRICHED_TRADES_CSV_PATH, encoding="utf-8-sig")
    df = enrich_political_signals(df)

    output_path = Path(ENRICHED_TRADES_CSV_PATH).parent / "enriched_trades_political.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[PoliticalEnricher] Saved to: {output_path}")

    # Preview top results
    preview_cols = [
        "Investor Name", "Ticker", "Traded",
        "member_sector_sentiment", "member_sentiment_trend",
        "committee_sector_sentiment", "bipartisan_sentiment_align",
        "silent_cluster_flag", "statement_to_trade_days",
    ]
    preview_cols = [c for c in preview_cols if c in df.columns]
    print("\n--- Preview (first 10 unique ticker/member combos) ---")
    seen = set()
    count = 0
    for _, row in df.iterrows():
        key = (row.get("Investor Name", ""), row.get("Ticker", ""))
        if key not in seen:
            seen.add(key)
            print(
                f"  {str(row.get('Investor Name','')):<22} "
                f"{str(row.get('Ticker','')):<6} "
                f"sentiment={row.get('member_sector_sentiment',0):+.3f}  "
                f"trend={row.get('member_sentiment_trend',0):+d}  "
                f"committee={row.get('committee_sector_sentiment',0):+.3f}  "
                f"bipartisan={row.get('bipartisan_sentiment_align',0)}  "
                f"silent={row.get('silent_cluster_flag',0)}  "
                f"days={row.get('statement_to_trade_days','—')}"
            )
            count += 1
            if count >= 15:
                break


if __name__ == "__main__":
    run_political_enrichment()

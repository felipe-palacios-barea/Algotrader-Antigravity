"""
Module: political_data_store.py

SQLite-backed storage manager for the Political Intelligence Pipeline.
Stores and organizes text signals from Congress.gov, official press releases,
and Threads posts for downstream stock/industry sentiment analysis.

Usage:
    from political_data_store import PoliticalDataStore
    store = PoliticalDataStore()
    store.insert_signal(source='press_release', member_name='John Boozman', ...)
    results = store.query_intelligence(ticker='NVDA', days_back=30)
"""

import sqlite3
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

from config import POLITICAL_INTELLIGENCE_DB


class PoliticalDataStore:
    """
    Manages a SQLite database of political intelligence signals.
    All inserts are idempotent — duplicate source URLs are silently ignored.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or POLITICAL_INTELLIGENCE_DB
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Create tables if they don't exist."""
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS intelligence_signals (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_hash            TEXT    UNIQUE,      -- SHA256 of source_url, prevents duplicates
                    source              TEXT    NOT NULL,    -- 'congress_gov', 'press_release', 'threads'
                    member_name         TEXT,               -- Normalized name (matches committees.csv)
                    posted_at           DATETIME,           -- Publication date/time
                    title               TEXT,               -- Headline or subject line
                    content_raw         TEXT,               -- Full original text (preserved for future NLP)
                    source_url          TEXT,               -- Original URL
                    enriched_at         DATETIME,           -- NULL until GPT enrichment runs
                    sentiment_score     REAL,               -- -1.0 (negative) to +1.0 (positive)
                    tickers_mentioned   TEXT,               -- Comma-separated, e.g. 'NVDA,MSFT'
                    industries_mentioned TEXT,              -- Comma-separated, e.g. 'Semiconductors,AI'
                    topic_sentiment     TEXT,               -- JSON: {"NVDA": 0.8, "AI Regulation": -0.5}
                    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS source_run_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    source          TEXT    NOT NULL,
                    member_name     TEXT    NOT NULL,
                    last_fetched_at DATETIME NOT NULL,
                    UNIQUE(source, member_name)
                );

                CREATE INDEX IF NOT EXISTS idx_member ON intelligence_signals(member_name);
                CREATE INDEX IF NOT EXISTS idx_source ON intelligence_signals(source);
                CREATE INDEX IF NOT EXISTS idx_posted_at ON intelligence_signals(posted_at);
                CREATE INDEX IF NOT EXISTS idx_enriched ON intelligence_signals(enriched_at);
            """)
        print(f"[DataStore] Initialized database at: {self.db_path}")

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    def insert_signal(
        self,
        source: str,
        member_name: str,
        posted_at: Optional[datetime],
        title: str,
        content_raw: str,
        source_url: str = "",
    ) -> bool:
        """
        Inserts a new signal. Returns True on success, False if duplicate (same URL).
        """
        url_hash = hashlib.sha256(source_url.encode()).hexdigest() if source_url else hashlib.sha256((member_name + title + str(posted_at)).encode()).hexdigest()

        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO intelligence_signals
                        (url_hash, source, member_name, posted_at, title, content_raw, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (url_hash, source, member_name, posted_at, title, content_raw, source_url),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    return True  # Inserted
                return False  # Duplicate
        except Exception as e:
            print(f"[DataStore] Insert error: {e}")
            return False

    # ------------------------------------------------------------------
    # Retrieve unenriched rows
    # ------------------------------------------------------------------

    def get_unenriched(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns rows where GPT enrichment hasn't run yet (enriched_at IS NULL)."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM intelligence_signals
                WHERE enriched_at IS NULL AND content_raw IS NOT NULL AND content_raw != ''
                ORDER BY posted_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Update enrichment results
    # ------------------------------------------------------------------

    def update_enrichment(
        self,
        signal_id: int,
        sentiment_score: float,
        tickers_mentioned: str,
        industries_mentioned: str,
        topic_sentiment: Dict[str, float],
    ):
        """Updates a row with GPT enrichment results and marks it as enriched."""
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE intelligence_signals
                SET enriched_at           = ?,
                    sentiment_score       = ?,
                    tickers_mentioned     = ?,
                    industries_mentioned  = ?,
                    topic_sentiment       = ?
                WHERE id = ?
                """,
                (
                    datetime.utcnow().isoformat(),
                    sentiment_score,
                    tickers_mentioned,
                    industries_mentioned,
                    json.dumps(topic_sentiment),
                    signal_id,
                ),
            )

    # ------------------------------------------------------------------
    # Main query interface for downstream analysis
    # ------------------------------------------------------------------

    def query_intelligence(
        self,
        member: Optional[str] = None,
        ticker: Optional[str] = None,
        industry: Optional[str] = None,
        source: Optional[str] = None,
        days_back: int = 30,
        only_enriched: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve signals matching the given filters.

        Examples:
            # Get all signals about NVDA in the last 30 days
            store.query_intelligence(ticker='NVDA')

            # Get all signals from John Boozman in the last 7 days
            store.query_intelligence(member='John Boozman', days_back=7)

            # Get all Semiconductor signals from press releases
            store.query_intelligence(industry='Semiconductors', source='press_release')
        """
        cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

        conditions = ["posted_at >= ?"]
        params: List[Any] = [cutoff]

        if member:
            conditions.append("member_name LIKE ?")
            params.append(f"%{member}%")

        if ticker:
            conditions.append("tickers_mentioned LIKE ?")
            params.append(f"%{ticker}%")

        if industry:
            conditions.append("industries_mentioned LIKE ?")
            params.append(f"%{industry}%")

        if source:
            conditions.append("source = ?")
            params.append(source)

        if only_enriched:
            conditions.append("enriched_at IS NOT NULL")

        where_clause = " AND ".join(conditions)

        with self._get_conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM intelligence_signals WHERE {where_clause} ORDER BY posted_at DESC",
                params,
            ).fetchall()

        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Run log (prevent re-fetching)
    # ------------------------------------------------------------------

    def update_run_log(self, source: str, member_name: str):
        """Mark that a source/member combo was just fetched."""
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO source_run_log (source, member_name, last_fetched_at)
                VALUES (?, ?, ?)
                """,
                (source, member_name, datetime.utcnow().isoformat()),
            )

    def was_recently_fetched(self, source: str, member_name: str, hours: int = 6) -> bool:
        """Returns True if this source/member was fetched within the last N hours."""
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT last_fetched_at FROM source_run_log
                WHERE source = ? AND member_name = ? AND last_fetched_at >= ?
                """,
                (source, member_name, cutoff),
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Returns a summary of what's in the database."""
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM intelligence_signals").fetchone()[0]
            enriched = conn.execute("SELECT COUNT(*) FROM intelligence_signals WHERE enriched_at IS NOT NULL").fetchone()[0]
            by_source = conn.execute("SELECT source, COUNT(*) as cnt FROM intelligence_signals GROUP BY source").fetchall()
        return {
            "total_signals": total,
            "enriched": enriched,
            "pending_enrichment": total - enriched,
            "by_source": {r["source"]: r["cnt"] for r in by_source},
        }


if __name__ == "__main__":
    store = PoliticalDataStore()
    print(store.get_stats())

"""
Module: political_intelligence_fetcher.py

Multi-source political intelligence data fetcher.
Uses a plugin-style BaseSource architecture so new sources can be added trivially.

Current Sources:
    - CongressGovSource   (Official Congress.gov API)
    - PressReleaseSource  (Web scraper for official .gov member sites)
    - ThreadsSource       (Meta Threads API)

Usage:
    python political_intelligence_fetcher.py
    
    Or call programmatically:
        from political_intelligence_fetcher import run_all_fetchers
        run_all_fetchers()
"""

import time
import re
import requests
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("[WARNING] beautifulsoup4 not installed. Press release scraping will be limited.")

from config import (
    CONGRESS_GOV_API_KEY,
    THREADS_ACCESS_TOKEN,
    MEMBER_HANDLES_CSV,
    INTELLIGENCE_FETCH_DAYS_BACK,
    INTELLIGENCE_PAUSE_SEC,
)
from political_data_store import PoliticalDataStore


# ==============================================================================
# Base Plugin Interface
# ==============================================================================

class BaseSource(ABC):
    """
    All fetchers must implement this interface.
    Adding a new source (Bluesky, Reddit, etc.) = create a new subclass.
    """
    SOURCE_NAME: str = "base"

    def __init__(self, store: PoliticalDataStore):
        self.store = store

    @abstractmethod
    def fetch(self, member: Dict) -> List[Dict]:
        """
        Fetch signals for a given member dict.
        member keys: member_name, congress_bioguide_id, threads_handle, party, state
        Returns a list of signal dicts with keys: title, content_raw, posted_at, source_url
        """
        pass

    def _pause(self):
        time.sleep(INTELLIGENCE_PAUSE_SEC)

    def _ingest(self, member: Dict, signals: List[Dict]) -> int:
        """Push fetched signals into the data store. Returns number of new inserts."""
        inserted = 0
        for sig in signals:
            ok = self.store.insert_signal(
                source=self.SOURCE_NAME,
                member_name=member["member_name"],
                posted_at=sig.get("posted_at"),
                title=sig.get("title", ""),
                content_raw=sig.get("content_raw", ""),
                source_url=sig.get("source_url", ""),
            )
            if ok:
                inserted += 1
        self.store.update_run_log(self.SOURCE_NAME, member["member_name"])
        return inserted


# ==============================================================================
# Source 1: Congress.gov API
# ==============================================================================

class CongressGovSource(BaseSource):
    """
    Fetches sponsored legislation and latest actions per member via Congress.gov v3 API.
    Bills have rich title + latestAction text that GPT can analyze for sector sentiment.
    API key from: https://api.data.gov/signup/ (free, ~5000 req/hour)
    """
    SOURCE_NAME = "congress_gov"
    BASE_URL = "https://api.congress.gov/v3"
    CURRENT_CONGRESS = 119  # 119th Congress (2025-2026)

    def fetch(self, member: Dict) -> List[Dict]:
        if not CONGRESS_GOV_API_KEY:
            return []

        bioguide_id = member.get("congress_bioguide_id", "").strip()
        if not bioguide_id:
            return []

        if self.store.was_recently_fetched(self.SOURCE_NAME, member["member_name"], hours=12):
            return []

        signals = []

        # ── Endpoint 1: Bills sponsored by this member ────────────────────────
        try:
            resp = requests.get(
                f"{self.BASE_URL}/member/{bioguide_id}/sponsored-legislation",
                params={
                    "api_key": CONGRESS_GOV_API_KEY,
                    "limit": 20,
                    "format": "json",
                },
                timeout=15,
            )
            self._pause()

            if resp.status_code == 200:
                bills = resp.json().get("sponsoredLegislation", [])
                for bill in bills:
                    title = bill.get("title", "").strip()
                    if not title or len(title) < 10:
                        continue

                    latest = bill.get("latestAction") or {}
                    action_text = latest.get("text", "") if isinstance(latest, dict) else ""
                    action_date = latest.get("actionDate", "") if isinstance(latest, dict) else ""

                    # Build a rich content blob for GPT
                    content = f"Bill: {title}"
                    if action_text:
                        content += f"\nLatest action: {action_text}"

                    intro_date = bill.get("introducedDate", action_date)
                    try:
                        posted = datetime.strptime(intro_date[:10], "%Y-%m-%d") if intro_date else None
                    except ValueError:
                        posted = None

                    # Skip bills older than our lookback window
                    if posted and posted < datetime.utcnow() - timedelta(days=INTELLIGENCE_FETCH_DAYS_BACK):
                        continue

                    bill_type = bill.get("type", "").lower()
                    bill_num = bill.get("number", "")
                    congress = bill.get("congress", self.CURRENT_CONGRESS)
                    url = f"https://congress.gov/bill/{congress}th-congress/{bill_type}/{bill_num}"

                    signals.append({
                        "title": title[:200],
                        "content_raw": content,
                        "posted_at": posted,
                        "source_url": url,
                    })

        except Exception as e:
            print(f"[CongressGov] Error fetching legislation for {member['member_name']}: {e}")

        # ── Endpoint 2: Cosponsored legislation (additional context) ──────────
        try:
            resp = requests.get(
                f"{self.BASE_URL}/member/{bioguide_id}/cosponsored-legislation",
                params={
                    "api_key": CONGRESS_GOV_API_KEY,
                    "limit": 10,
                    "format": "json",
                },
                timeout=15,
            )
            self._pause()

            if resp.status_code == 200:
                bills = resp.json().get("cosponsoredLegislation", [])
                for bill in bills:
                    title = bill.get("title", "").strip()
                    if not title or len(title) < 10:
                        continue

                    latest = bill.get("latestAction") or {}
                    action_text = latest.get("text", "") if isinstance(latest, dict) else ""
                    action_date = latest.get("actionDate", "") if isinstance(latest, dict) else ""
                    content = f"[Cosponsored] {title}"
                    if action_text:
                        content += f"\nLatest action: {action_text}"

                    intro_date = bill.get("introducedDate", action_date)
                    try:
                        posted = datetime.strptime(intro_date[:10], "%Y-%m-%d") if intro_date else None
                    except ValueError:
                        posted = None

                    if posted and posted < datetime.utcnow() - timedelta(days=INTELLIGENCE_FETCH_DAYS_BACK):
                        continue

                    bill_type = bill.get("type", "").lower()
                    bill_num = bill.get("number", "")
                    congress = bill.get("congress", self.CURRENT_CONGRESS)
                    url = f"https://congress.gov/bill/{congress}th-congress/{bill_type}/{bill_num}"

                    signals.append({
                        "title": title[:200],
                        "content_raw": content,
                        "posted_at": posted,
                        "source_url": url,
                    })

        except Exception as e:
            print(f"[CongressGov] Error fetching cosponsorships for {member['member_name']}: {e}")

        return signals


# ==============================================================================
# Source 2: Official Press Release Scraper
# ==============================================================================

class RSSPressReleaseSource(BaseSource):
    """
    Fetches press releases from official congressional RSS feeds.
    RSS feeds are publicly available and bypass Senate WAF restrictions.
    Falls back to GovInfo.gov when RSS is not found.
    """
    SOURCE_NAME = "press_release"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; CongressResearchBot/1.0; research)",
    }

    # Most senators and representatives publish RSS feeds at predictable URLs.
    # Slug = last name (lowercase). Special cases handled via RSS_OVERRIDES in member_handles.csv.
    RSS_PATTERNS = [
        # Senate patterns (.cfm RSS — most senators)
        "https://www.{slug}.senate.gov/public/?p=PressReleases&ContentType=application/rss+xml",
        "https://www.{slug}.senate.gov/public/index.cfm?p=PressReleases&ContentType=application/rss+xml",
        "https://{slug}.senate.gov/rss/feeds/?type=press-releases",
        "https://www.{slug}.senate.gov/news/press-releases/feed",
        # House patterns
        "https://{slug}.house.gov/rss.xml",
        "https://{slug}.house.gov/press-releases/feed",
        "https://{slug}.house.gov/press?format=xml",
    ]

    def _get_slug(self, member: Dict) -> str:
        """Return the RSS slug for this member — uses press_release_slug if set, else last name."""
        # If the member_handles.csv has a 'press_release_slug' column, use it
        override = member.get("press_release_slug", "").strip()
        if override:
            return override
        name = member.get("member_name", "")
        parts = name.lower().split()
        return parts[-1] if parts else ""

    def _try_rss(self, url: str) -> Optional[str]:
        """Fetch RSS/Atom feed XML. Returns text or None."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            ct = resp.headers.get("content-type", "")
            if resp.status_code == 200 and ("xml" in ct or "rss" in ct or resp.text.strip().startswith("<")):
                return resp.text
        except Exception:
            pass
        return None

    def _parse_rss(self, xml_text: str, member_name: str) -> List[Dict]:
        """Parse RSS/Atom XML into signal dicts."""
        if not BS4_AVAILABLE:
            return []

        signals = []
        try:
            # Use lxml-xml parser for RSS; fall back to html.parser
            try:
                soup = BeautifulSoup(xml_text, "lxml-xml")
            except Exception:
                soup = BeautifulSoup(xml_text, "html.parser")

            items = soup.find_all("item") or soup.find_all("entry")
            cutoff = datetime.utcnow() - timedelta(days=INTELLIGENCE_FETCH_DAYS_BACK)

            for item in items:
                title_tag = item.find("title")
                title = title_tag.get_text(strip=True) if title_tag else ""
                if not title or len(title) < 10:
                    continue

                # Content: try description, summary, content:encoded
                content_tag = (
                    item.find("description")
                    or item.find("summary")
                    or item.find("content")
                )
                content_text = content_tag.get_text(strip=True) if content_tag else title
                # Strip residual HTML tags from content
                content_text = re.sub(r"<[^>]+>", " ", content_text).strip()
                content_text = content_text[:3000]  # Cap for GPT

                # Date
                pub_tag = item.find("pubDate") or item.find("published") or item.find("updated")
                pub_str = pub_tag.get_text(strip=True) if pub_tag else ""
                posted = None
                for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
                    try:
                        posted = datetime.strptime(pub_str[:30].strip(), fmt).replace(tzinfo=None)
                        break
                    except ValueError:
                        continue

                if posted is None:
                    posted = datetime.utcnow()

                if posted < cutoff:
                    continue  # Too old

                link_tag = item.find("link")
                url = ""
                if link_tag:
                    url = link_tag.get("href") or link_tag.get_text(strip=True)

                signals.append({
                    "title": title[:200],
                    "content_raw": content_text if content_text else title,
                    "posted_at": posted,
                    "source_url": url,
                })

        except Exception as e:
            print(f"[RSS] Parse error for {member_name}: {e}")

        return signals

    def fetch(self, member: Dict) -> List[Dict]:
        # Senate.gov enforces WAF that blocks all automated requests (403).
        # House.gov has no WAF — RSS works perfectly at {slug}.house.gov/rss.xml
        chamber = member.get("chamber", "").lower()
        if "senate" in chamber:
            # Senate press releases are captured via CongressGovSource (sponsored-legislation)
            return []

        if self.store.was_recently_fetched(self.SOURCE_NAME, member["member_name"], hours=12):
            return []

        slug = self._get_slug(member)
        if not slug:
            return []

        # House-specific patterns (confirmed working)
        house_patterns = [
            f"https://{slug}.house.gov/rss.xml",
            f"https://{slug}.house.gov/press-releases/feed",
            f"https://{slug}.house.gov/news/press-releases/feed",
        ]

        for url in house_patterns:
            xml = self._try_rss(url)
            self._pause()
            if xml:
                signals = self._parse_rss(xml, member["member_name"])
                if signals:
                    print(f"[RSS] {member['member_name']}: +{len(signals)} press releases from {url}")
                    self.store.update_run_log(self.SOURCE_NAME, member["member_name"])
                    return signals

        print(f"[RSS] No feed found for {member['member_name']} (slug: {slug}, chamber: {chamber})")
        self.store.update_run_log(self.SOURCE_NAME, member["member_name"])
        return []


# ==============================================================================
# Source 3: Meta Threads API
# ==============================================================================

class ThreadsSource(BaseSource):
    """
    Fetches recent posts from a member's Threads account via the official Meta API.
    Requires a valid THREADS_ACCESS_TOKEN and the member's handle in member_handles.csv.
    """
    SOURCE_NAME = "threads"
    BASE_URL = "https://graph.threads.net/v1.0"

    def fetch(self, member: Dict) -> List[Dict]:
        if not THREADS_ACCESS_TOKEN:
            return []

        handle = member.get("threads_handle", "").strip().lstrip("@")
        if not handle:
            return []  # This member has no Threads account registered

        if self.store.was_recently_fetched(self.SOURCE_NAME, member["member_name"], hours=6):
            return []

        signals = []
        cutoff = (datetime.utcnow() - timedelta(days=INTELLIGENCE_FETCH_DAYS_BACK)).strftime("%s")

        try:
            resp = requests.get(
                f"{self.BASE_URL}/me/threads",
                params={
                    "access_token": THREADS_ACCESS_TOKEN,
                    "fields": "id,text,timestamp,permalink",
                    "since": cutoff,
                    "limit": 100,
                },
                timeout=15,
            )
            self._pause()

            if resp.status_code != 200:
                print(f"[Threads] API error {resp.status_code} for {member['member_name']}: {resp.text[:200]}")
                return []

            data = resp.json().get("data", [])
            for post in data:
                text = post.get("text", "")
                ts = post.get("timestamp", "")
                try:
                    posted = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                except (ValueError, TypeError):
                    posted = datetime.utcnow()

                signals.append({
                    "title": text[:100],
                    "content_raw": text,
                    "posted_at": posted,
                    "source_url": post.get("permalink", ""),
                })

        except Exception as e:
            print(f"[Threads] Error for {member['member_name']}: {e}")

        return signals


# ==============================================================================
# Orchestrator
# ==============================================================================

def load_members() -> List[Dict]:
    """Load member list from member_handles.csv."""
    if not Path(MEMBER_HANDLES_CSV).exists():
        print(f"[ERROR] member_handles.csv not found at {MEMBER_HANDLES_CSV}")
        return []
    df = pd.read_csv(MEMBER_HANDLES_CSV, dtype=str, keep_default_na=False)
    df = df.drop_duplicates(subset=["member_name"])
    return df.to_dict(orient="records")


def run_all_fetchers(members: Optional[List[Dict]] = None):
    """
    Main entry point. Runs all fetchers for all members and inserts results.
    Pass a filtered `members` list to run on a subset.
    """
    store = PoliticalDataStore()
    members = members or load_members()

    # Register all sources here — adding a new one is as simple as appending
    sources: List[BaseSource] = [
        CongressGovSource(store),
        RSSPressReleaseSource(store),
        ThreadsSource(store),
    ]

    total_new = 0
    print(f"\n[Fetcher] Starting run for {len(members)} members across {len(sources)} sources.")

    for member in members:
        name = member.get("member_name", "Unknown")
        for src in sources:
            try:
                signals = src.fetch(member)
                new = src._ingest(member, signals)
                if new > 0:
                    print(f"  [{src.SOURCE_NAME}] {name}: +{new} new signals")
                total_new += new
            except Exception as e:
                print(f"  [{src.SOURCE_NAME}] Error for {name}: {e}")

    print(f"\n[Fetcher] Done. {total_new} new signals inserted.")
    print(f"[Fetcher] Database stats: {store.get_stats()}")


if __name__ == "__main__":
    run_all_fetchers()

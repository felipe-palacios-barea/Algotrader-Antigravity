"""
Module: intelligence_enricher.py

GPT-powered enrichment for the Political Intelligence Pipeline.
Reads unenriched rows from political_intelligence.db and extracts:
  - Overall sentiment score (-1.0 to +1.0)
  - Tickers and industries mentioned
  - Per-topic/per-ticker sentiment (JSON)

Compatible with the existing ai_evaluator.py pattern (same OpenAI client setup).

Usage:
    python intelligence_enricher.py
    
    Or programmatically:
        from intelligence_enricher import enrich_pending
        enrich_pending()
"""

import json
import time
from openai import OpenAI

from config import OPENAI_API_KEY, MODEL_NAME
from political_data_store import PoliticalDataStore

# Initialize OpenAI client (same as ai_evaluator.py)
client = OpenAI(api_key=OPENAI_API_KEY)

# GPT batch size — process this many rows per run to manage costs
BATCH_SIZE = 50
PAUSE_SEC = 0.5  # Small delay between API calls


def _build_prompt(member_name: str, title: str, content: str) -> str:
    """Build the structured prompt for GPT to extract political intelligence signals."""
    return (
        f"You are a financial and political intelligence analyst.\n\n"
        f"A US Congress member named {member_name} published the following statement:\n"
        f"TITLE: {title}\n"
        f"CONTENT: {content[:2000]}\n\n"  # Truncate to manage token cost
        f"Analyze this statement and return a JSON object with exactly these keys:\n"
        f"1. \"sentiment_score\": a float from -1.0 (very negative) to +1.0 (very positive) "
        f"representing their overall sentiment toward economic activity / markets.\n"
        f"2. \"tickers_mentioned\": a list of US stock tickers explicitly OR implicitly mentioned "
        f"(e.g., if they mention 'Boeing', include 'BA'). Empty list if none.\n"
        f"3. \"industries_mentioned\": a list of industry names (e.g., 'Defense', 'Semiconductors', "
        f"'Pharmaceuticals', 'Energy'). Empty list if none.\n"
        f"4. \"topic_sentiment\": a dict mapping each ticker/industry to its specific sentiment float. "
        f"E.g., {{\"NVDA\": 0.8, \"AI Regulation\": -0.3}}. Empty dict if none.\n"
        f"5. \"summary\": one sentence summarizing the key market-relevant insight.\n\n"
        f"Respond only with valid JSON. No markdown."
    )


def _call_gpt(member_name: str, title: str, content: str) -> dict:
    """Call GPT and return structured enrichment data."""
    prompt = _build_prompt(member_name, title, content)
    max_retries = 3

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You are a financial and political data analyst. Output valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            raw = response.choices[0].message.content.strip()

            # Strip markdown fences if present (same defensive logic as ai_evaluator.py)
            if raw.startswith("```"):
                lines = raw.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                raw = "\n".join(lines).strip()

            data = json.loads(raw)
            return {
                "sentiment_score": float(data.get("sentiment_score", 0.0)),
                "tickers_mentioned": ",".join(data.get("tickers_mentioned", [])),
                "industries_mentioned": ",".join(data.get("industries_mentioned", [])),
                "topic_sentiment": data.get("topic_sentiment", {}),
                "summary": data.get("summary", ""),
            }

        except Exception as e:
            err_str = str(e)
            if "429" in err_str and attempt < max_retries - 1:
                print(f"  Rate limit hit. Waiting 65s before retry (attempt {attempt + 1}/{max_retries})...")
                time.sleep(65)
                continue
            print(f"  GPT error: {err_str}")
            return {
                "sentiment_score": 0.0,
                "tickers_mentioned": "",
                "industries_mentioned": "",
                "topic_sentiment": {},
                "summary": f"Error: {err_str[:100]}",
            }

    return {
        "sentiment_score": 0.0,
        "tickers_mentioned": "",
        "industries_mentioned": "",
        "topic_sentiment": {},
        "summary": "Exceeded max retries.",
    }


def enrich_pending(batch_size: int = BATCH_SIZE):
    """
    Main entry point. Enriches up to `batch_size` unenriched rows from the database.
    Each row is billed only once (enriched_at is set immediately on success).
    """
    store = PoliticalDataStore()
    rows = store.get_unenriched(limit=batch_size)

    if not rows:
        print("[Enricher] No pending rows to enrich.")
        return

    print(f"[Enricher] Enriching {len(rows)} rows with GPT ({MODEL_NAME})...")
    enriched_count = 0

    for row in rows:
        signal_id = row["id"]
        member_name = row.get("member_name", "Unknown Congress Member")
        title = row.get("title", "")
        content = row.get("content_raw", "")

        if not content.strip() and not title.strip():
            # Nothing to analyze — mark as enriched with neutral defaults to avoid re-processing
            store.update_enrichment(signal_id, 0.0, "", "", {})
            continue

        result = _call_gpt(member_name, title, content)
        store.update_enrichment(
            signal_id=signal_id,
            sentiment_score=result["sentiment_score"],
            tickers_mentioned=result["tickers_mentioned"],
            industries_mentioned=result["industries_mentioned"],
            topic_sentiment=result["topic_sentiment"],
        )

        enriched_count += 1
        ticker_str = result["tickers_mentioned"] or "—"
        industry_str = result["industries_mentioned"] or "—"
        print(
            f"  [{enriched_count}/{len(rows)}] {member_name}: "
            f"score={result['sentiment_score']:+.2f}, tickers={ticker_str}, industries={industry_str}"
        )

        time.sleep(PAUSE_SEC)

    print(f"\n[Enricher] Done. Enriched {enriched_count} rows.")
    print(f"[Enricher] Database stats: {store.get_stats()}")


# ==============================================================================
# Quick query helper (for notebooks / downstream pipeline)
# ==============================================================================

def get_sentiment_for_ticker(ticker: str, days_back: int = 30):
    """
    Convenience function: get all enriched signals mentioning a specific ticker.
    Returns a list of dicts sorted by posted_at descending.

    Example:
        signals = get_sentiment_for_ticker('NVDA', days_back=14)
        for s in signals:
            print(s['member_name'], s['sentiment_score'], s['posted_at'])
    """
    store = PoliticalDataStore()
    return store.query_intelligence(ticker=ticker, days_back=days_back, only_enriched=True)


def get_sentiment_for_industry(industry: str, days_back: int = 30):
    """
    Convenience function: get all enriched signals mentioning a specific industry.

    Example:
        signals = get_sentiment_for_industry('Defense', days_back=30)
    """
    store = PoliticalDataStore()
    return store.query_intelligence(industry=industry, days_back=days_back, only_enriched=True)


def get_member_sentiment_profile(member_name: str, days_back: int = 90):
    """
    Convenience function: get all enriched signals from a specific congress member.

    Example:
        profile = get_member_sentiment_profile('John Boozman', days_back=60)
    """
    store = PoliticalDataStore()
    return store.query_intelligence(member=member_name, days_back=days_back, only_enriched=True)


if __name__ == "__main__":
    enrich_pending()

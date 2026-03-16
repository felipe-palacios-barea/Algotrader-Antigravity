"""
Module: main.py

Master orchestrator for the Congressional Algotrader pipeline.
Executes the data collection, formatting, fuzzy matching, and evaluation steps.
"""
from trades import fetch_latest_trades
from committees import fetch_committees
from fuzzy_match import match_investors
from fmp_recommendation import augment_recommendations
from fmp_standard_dev import augment_price_change
from ai_evaluator import evaluate_trades
from signal_enricher import enrich_signals
from config import ENRICHED_TRADES_CSV_PATH, CONGRESS_GOV_API_KEY


def main():
    print("--- Starting Congressional Algotrader Pipeline ---")
    
    # 0. (Optional) Fetch Political Intelligence signals
    if CONGRESS_GOV_API_KEY:
        print("\n[0/7] Fetching political intelligence (press releases, Congress.gov, Threads)...")
        try:
            from political_intelligence_fetcher import run_all_fetchers
            from intelligence_enricher import enrich_pending
            run_all_fetchers()
            enrich_pending(batch_size=25)
            print("-> Political intelligence updated.")
        except Exception as e:
            print(f"-> Political intelligence step failed (non-critical): {e}")
    
    # 1. Fetch Latest Trades
    print("\n[1/7] Fetching latest trades...")
    trades_df = fetch_latest_trades()
    print(f"-> Fetched {len(trades_df)} new trades.")

    # 2. Fetch Committees
    print("\n[2/7] Fetching committee memberships...")
    committees_df = fetch_committees()
    print(f"-> Fetched {len(committees_df)} new committee entries.")
    
    # 3. Match Investors to Committees
    print("\n[3/7] Mapping investors to their committees/subcommittees...")
    matched_df = match_investors(trades_df, committees_df)
    matched_df.to_csv(ENRICHED_TRADES_CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"-> Generated enriched trades matched data.")
    
    # 4. Fetch Analyst Recommendations from FMP
    print("\n[4/7] Fetching FMP analyst recommendations...")
    try:
        updated_recs = augment_recommendations(trades_path=ENRICHED_TRADES_CSV_PATH)
        print("-> Added FMP recommendations.")
    except Exception as e:
        print(f"-> Failed finding recommendations: {e}")

    # 5. Fetch Price Metrics from FMP
    print("\n[5/7] Fetching FMP price metrics (current price, std dev)...")
    try:
        updated_prices = augment_price_change(trades_path=ENRICHED_TRADES_CSV_PATH)
        print("-> Added price metrics.")
    except Exception as e:
        print(f"-> Failed finding price metrics: {e}")
        
    # 6. Signal Enrichment (Clusters, Anomaly, Hearings)
    print("\n[6/7] Enriching with advanced signals (Clusters, Anomaly, Hearings)...")
    enrich_signals()
    
    # 7. Evaluate Trade Probability with Gemini
    print("\n[7/7] Evaluating insider trading probability with Google Gemini...")
    evaluate_trades()
    print("-> Evaluation complete.")
    
    print("\n--- Pipeline Execution Complete ---")


if __name__ == "__main__":
    main()

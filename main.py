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
from config import ENRICHED_TRADES_CSV_PATH


def main():
    print("--- Starting Congressional Algotrader Pipeline ---")
    
    # 1. Fetch Latest Trades
    print("\\n[1/6] Fetching latest trades...")
    trades_df = fetch_latest_trades()
    print(f"-> Fetched {len(trades_df)} new trades.")

    # 2. Fetch Committees
    print("\\n[2/6] Fetching committee memberships...")
    committees_df = fetch_committees()
    print(f"-> Fetched {len(committees_df)} new committee entries.")
    
    # 3. Match Investors to Committees
    print("\\n[3/6] Mapping investors to their committees/subcommittees...")
    matched_df = match_investors(trades_df, committees_df)
    matched_df.to_csv(ENRICHED_TRADES_CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"-> Generated enriched trades matched data.")
    
    # 4. Fetch Analyst Recommendations from FMP
    print("\\n[4/6] Fetching FMP analyst recommendations...")
    try:
        updated_recs = augment_recommendations(trades_path=ENRICHED_TRADES_CSV_PATH)
        print("-> Added FMP recommendations.")
    except Exception as e:
        print(f"-> Failed finding recommendations: {e}")

    # 5. Fetch Price Metrics from FMP
    print("\\n[5/6] Fetching FMP price metrics (current price, std dev)...")
    try:
        updated_prices = augment_price_change(trades_path=ENRICHED_TRADES_CSV_PATH)
        print("-> Added price metrics.")
    except Exception as e:
        print(f"-> Failed finding price metrics: {e}")
        
    # 6. Evaluate Trade Probability with Gemini
    print("\\n[6/6] Evaluating insider trading probability with Google Gemini...")
    evaluate_trades()
    print("-> Evaluation complete.")
    
    print("\\n--- Pipeline Execution Complete ---")


if __name__ == "__main__":
    main()

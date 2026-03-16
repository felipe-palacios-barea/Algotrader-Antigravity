"""
Module: signal_enricher.py

Enriches congressional trade data with advanced predictive signals:
1. Coordinated Cluster Trading (Bipartisan and Committee-based).
2. Filing Delay Anomaly (Z-score based on personal history).
3. Committee Hearing Proximity (To be integrated with hearing_scraper).
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import timedelta
from pathlib import Path
from config import ENRICHED_TRADES_CSV_PATH, TRADES_CSV_PATH

def calculate_cluster_signals(df: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
    """
    Identifies clusters where multiple politicians trade the same ticker within a window.
    - cluster_bipartisan: count of unique parties in the cluster.
    - cluster_committee: count of unique committees involved in the cluster.
    """
    df = df.copy()
    df['Traded'] = pd.to_datetime(df['Traded'])
    
    # Sort for rolling window logic
    df = df.sort_values('Traded')
    
    # Initialize columns
    df['cluster_count'] = 0
    df['cluster_bipartisan'] = 0
    df['cluster_committee'] = 0

    tickers = df['Ticker'].dropna().unique()
    
    for ticker in tickers:
        ticker_mask = df['Ticker'] == ticker
        ticker_df = df[ticker_mask].copy()
        
        for idx, row in ticker_df.iterrows():
            trade_date = row['Traded']
            # Window: [trade_date - window, trade_date + window]
            window_mask = (ticker_df['Traded'] >= trade_date - timedelta(days=window_days)) & \
                          (ticker_df['Traded'] <= trade_date + timedelta(days=window_days))
            
            cluster = ticker_df[window_mask]
            
            unique_politicians = cluster['Investor Name'].nunique()
            unique_parties = cluster['Party'].nunique()
            
            # For committee clusters, we need to handle the semi-colon separated string
            committees = []
            if 'Committees' in cluster.columns:
                for comm_str in cluster['Committees'].dropna():
                    committees.extend([c.strip() for c in str(comm_str).split(';') if c.strip()])
            unique_committees = len(set(committees))

            df.at[idx, 'cluster_count'] = unique_politicians
            df.at[idx, 'cluster_bipartisan'] = 1 if unique_parties > 1 else 0
            df.at[idx, 'cluster_committee'] = 1 if unique_committees > 1 else 0
            
    return df

def calculate_filing_anomaly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the Z-score of the filing delay for each politician.
    Delay = (Published - Traded).
    """
    df = df.copy()
    df['Traded'] = pd.to_datetime(df['Traded'])
    df['Published'] = pd.to_datetime(df['Published'])
    
    # Ensure Filing Time exists or recalculate
    df['filing_delay_actual'] = (df['Published'] - df['Traded']).dt.days
    
    # Group by politician to get means and std devs
    stats = df.groupby('Investor Name')['filing_delay_actual'].agg(['mean', 'std']).reset_index()
    stats.columns = ['Investor Name', 'politician_delay_mean', 'politician_delay_std']
    
    # Drop existing mean/std if they exist to avoid suffixing
    cols_to_drop = [c for c in ['politician_delay_mean', 'politician_delay_std'] if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    df = df.merge(stats, on='Investor Name', how='left')
    
    # Calculate Z-score: (x - mean) / std. Fill 0 std with 0 z-score.
    df['delay_zscore'] = (df['filing_delay_actual'] - df['politician_delay_mean']) / df['politician_delay_std']
    df['delay_zscore'] = df['delay_zscore'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Clean up temp columns
    # df.drop(columns=['politician_delay_mean', 'politician_delay_std'], inplace=True)
    
    return df

def calculate_hearing_proximity(df: pd.DataFrame, hearings_csv: Path, window_days: int = 7) -> pd.DataFrame:
    """
    Flags trades that occur within window_days of a relevant committee hearing.
    Matches primarily on Committee name.
    """
    df = df.copy()
    if not hearings_csv.exists():
        df['hearing_proximity_alert'] = 0
        return df

    hearings = pd.read_csv(hearings_csv)
    hearings['Date_Parsed'] = pd.to_datetime(hearings['Date_Parsed'])
    df['Traded'] = pd.to_datetime(df['Traded'])

    df['hearing_proximity_alert'] = 0

    for idx, row in df.iterrows():
        trade_date = row['Traded']
        # If the politician has committees listed
        if pd.isna(row.get('Committees')):
            continue
            
        politician_committees = [c.strip().lower() for c in str(row['Committees']).split(';') if c.strip()]
        
        # Window: [trade_date - window, trade_date + window]
        relevant_hearings = hearings[
            (hearings['Date_Parsed'] >= trade_date - timedelta(days=window_days)) &
            (hearings['Date_Parsed'] <= trade_date + timedelta(days=window_days))
        ]
        
        if not relevant_hearings.empty:
            # Check if any hearing committee matches political committee
            for _, h_row in relevant_hearings.iterrows():
                h_comm = str(h_row['Committee']).lower()
                if any(h_comm in p_comm or p_comm in h_comm for p_comm in politician_committees):
                    df.at[idx, 'hearing_proximity_alert'] = 1
                    break
                    
    return df

def calculate_cpi_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the Congress Popularity Index (CPI).
    CPI = (Ticker_Congress_Frequency / Total_Trades) / (Ticker_Market_Cap / Total_US_Market_Cap)
    """
    df = df.copy()
    tickers = df['Ticker'].dropna().unique()
    total_trades = len(df)
    
    # Approx total US market cap (used for scaling)
    TOTAL_US_MARKET_CAP = 50_000_000_000_000 # ~50 Trillion USD
    
    print(f"Fetching market caps for {len(tickers)} tickers...")
    mcap_map = {}
    for i, t in enumerate(tickers):
        if t == 'N/A': continue
        try:
            if i % 10 == 0:
                print(f"  [{i}/{len(tickers)}] Ticker: {t}")
            stock = yf.Ticker(t)
            mcap = stock.info.get('marketCap')
            if mcap:
                mcap_map[t] = float(mcap)
        except Exception:
            pass

    # Ticker frequencies
    freq = df['Ticker'].value_counts().to_dict()
    
    def get_cpi(ticker):
        if pd.isna(ticker) or ticker == 'N/A':
            return 0
        
        t_freq = freq.get(ticker, 0)
        t_mcap = mcap_map.get(ticker)
        
        if not t_mcap or t_mcap == 0:
            return 0
            
        congress_share = t_freq / total_trades
        market_share = t_mcap / TOTAL_US_MARKET_CAP
        
        # CPI = How many times more popular is this in Congress vs the real world?
        return congress_share / market_share if market_share > 0 else 0

    df['congress_popularity_score'] = df['Ticker'].apply(get_cpi)
    return df

def enrich_signals():
    """
    Main entry point to load data, apply enrichments, and save.
    """
    if not ENRICHED_TRADES_CSV_PATH.exists():
        print(f"File not found: {ENRICHED_TRADES_CSV_PATH}")
        return

    df = pd.read_csv(ENRICHED_TRADES_CSV_PATH, encoding='utf-8-sig')
    
    # 1. Scraping latest hearings first
    print("Fetching latest hearings data...")
    from hearing_scraper import scrape_congress_hearings
    scrape_congress_hearings()
    
    print("Enriching Cluster Trading signals...")
    df = calculate_cluster_signals(df)
    
    print("Enriching Filing Anomaly signals...")
    df = calculate_filing_anomaly(df)

    print("Enriching Hearing Proximity signals...")
    hearings_path = Path("hearings.csv")
    df = calculate_hearing_proximity(df, hearings_path)

    print("Enriching Congress Popularity Index (CPI)...")
    df = calculate_cpi_signals(df)

    print("Enriching Political Intelligence signals (sentiment, clusters, timing)...")
    try:
        from political_signal_enricher import enrich_political_signals
        df = enrich_political_signals(df)
    except Exception as e:
        print(f"  -> Political signal enrichment skipped (non-critical): {e}")

    # Save back
    output_path = "enriched_trades_enriched.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Successfully enriched {len(df)} trades with advanced signals. Saved to: {output_path}")

if __name__ == "__main__":
    enrich_signals()

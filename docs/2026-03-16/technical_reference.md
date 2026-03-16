# Technical Data Reference
> Last Updated: 2026-03-16

This document provides a field-by-field breakdown of the primary data files used in the Congressional Algotrader pipeline.

## 1. `trades.csv` / `historical_trades.csv`
The raw ledger of scraped congressional trades.
- **`Published`**: Date the trade was publicly disclosed.
- **`Traded`**: Date the trade actually occurred.
- **`Filing Time`**: Number of days between Trade and Publication.
- **`Investor Name`**: Full name of the congressperson.
- **`Party`**: Political party (Democratic, Republican, etc.).
- **`Chamber`**: House or Senate.
- **`State`**: US State of the politician.
- **`Ticker`**: Stock symbol.
- **`Operation`**: Type of trade (Buy, Sell, Exchange).
- **`Low Amount` / `High Amount`**: Estimated USD range of the trade value.
- **`Price`**: Stock price at the time of the trade.

## 2. `committees.csv`
Scraped membership lists for House and Senate committees.
- **`Committee`**: Name of the standing committee.
- **`Subcommittee`**: Name of the subcommittee (if applicable).
- **`Full_Name`**: Standardized name of the member.
- **`State`**: State code for the member.

## 3. `enriched_trades.csv`
The primary output of the matching and enrichment pipeline. Contains all raw trade fields plus:
- **`Matched_Name`**: The standardized name found in committee data.
- **`Committees` / `Subcommittees`**: Semi-colon separated list of memberships.
- **`Recommendation`**: Latest analyst consensus rating (Buy/Hold/Sell).
- **`Current_Price`**: 5-day average historical close.
- **`Pct_Change`**: Return relative to entry price.
- **`Std_Dev_Away`**: Multiples of standard deviation from the mean price.
- **`Sector_Momentum`**: Returns of the relevant Sector ETF (e.g., XLK for Tech).
- **`cluster_count`**: Number of politicians trading this stock recently.
- **`delay_zscore`**: Statistical anomaly score for filing delays.
- **`hearing_proximity_alert`**: Flag (1/0) for nearby relevant hearings.
- **`probability`**: AI-calculated insider signal strength (0-1).
- **`lobbying_impact`**: AI score for industry lobbying depth.
- **`trump_interest`**: AI score for alignment with current policy trends.
- **`politician_alpha`**: Historical performance score derived from historical data.
- **`trade_size_conviction`**: AI score for trade significance (size vs net worth).

## 4. `politician_alpha_metrics.csv`
Aggregated performance stats per politician.
- **`Total_Positions`**: Number of trades analyzed for this person.
- **`Avg_Stock_Return_1Y`**: Mean return of their stock picks over the last year.
- **`Average_Alpha_1Y`**: Net performance vs. S&P 500.
- **`Alpha_Score_0_to_1`**: Normalized ranking used for weighting signals.

# Congressional Algotrader Project Documentation
> Last Updated: 2026-03-16

This document provides a comprehensive overview of the Congressional Algotrader project, explaining the project structure, data flow, and the logic behind every calculated value.

## Project Overview

The Congressional Algotrader is an automated pipeline designed to track, analyze, and evaluate stock trades made by members of the U.S. Congress. It identifies potential "insider signals" by cross-referencing trades with committee memberships, hearing schedules, and advanced statistical anomalies.

## Project Structure

The project consists of several modular Python scripts, each responsible for a specific stage of the pipeline:

- **`main.py`**: The master orchestrator that runs the entire pipeline in sequence.
- **`trades.py`**: Scrapes the latest trade data from CapitolTrades.com.
- **`committees.py`**: Scrapes House and Senate committee/subcommittee memberships.
- **`fuzzy_match.py`**: Matches investor names from trades to their respective committees.
- **`fmp_recommendation.py`**: Augments trade data with analyst recommendations from Yahoo Finance.
- **`fmp_standard_dev.py`**: Augments trade data with real-time price metrics and volatility stats.
- **`signal_enricher.py`**: Calculates advanced signals like clusters, filing delays, and hearing proximity.
- **`ai_evaluator.py`**: Uses Google Gemini (via OpenAI-compatible API) to assess the "insider" probability of each trade.
- **`alpha_calculator.py`**: Calculates the historical performance (Alpha) of each politician vs. the SPY benchmark.
- **`config.py`**: Centralized configuration for API keys, file paths, and URLs.

## Data Flow

1. **Collection**: Latest trades and committee memberships are scraped from web sources.
2. **Matching**: Investors are mapped to committees using a hybrid exact/fuzzy matching algorithm.
3. **Market Augmentation**: Current stock prices, sector performance, and analyst ratings are fetched.
4. **Signal Enrichment**:
    - **Clusters**: Finding multiple politicians trading the same stock.
    - **Anomalies**: Identifying unusual delays in trade reporting.
    - **Hearings**: Matching trades to committee hearing dates.
5. **Performance Analysis**: Historical Alpha is calculated to weight the "conviction" of the investor.
6. **AI Evaluation**: The LLM synthesizes all the above data to provide a final probability score and reasoning.

## Calculation Methodologies

### 1. Market & Price Metrics (`fmp_standard_dev.py`)
- **`Current_Price`**: Calculated as the 5-day average closing price using `yfinance`.
- **`Pct_Change`**: `((Current_Price - Trade_Price) / Trade_Price) * 100`.
- **`Std_Dev`**: The standard deviation of percentage changes over a 5-day historical window.
- **`Std_Dev_Away`**: `(Current_Price - 5_Day_Mean) / 5_Day_Std_Dev`. This indicates how much the current price deviates from its recent average.

### 2. Advanced Predictive Signals (`signal_enricher.py`)
- **`cluster_count`**: The number of unique politicians trading the same ticker within a ±30-day window.
- **`cluster_bipartisan`**: A boolean (1 or 0) indicating if the cluster includes members from more than one party.
- **`cluster_committee`**: A boolean indicating if members from multiple different committees are in the cluster.
- **`delay_zscore`**: Calculated per investor: `(Current_Reporting_Delay - Investor_Mean_Delay) / Investor_Std_Dev_Delay`. A high Z-score suggests an unusually long (or short) delay for that specific person.
- **`hearing_proximity_alert`**: A boolean (1 or 0) triggered if a trade occurs within ±7 days of a committee hearing that covers the relevant sector or stock.

### 3. Investor Performance Metrics (`alpha_calculator.py`)
- **`Average_Alpha_1Y`**: The 1-year rolling Alpha. It's the difference between the investor's weighted stock return and the SPY return over the same period: `Alpha = Stock_Return - SPY_Return`.
- **`Alpha_Score_0_to_1`**: A normalized version of Alpha. It clips Alpha at ±50% and maps it to a 0.0 to 1.0 scale (where 0.5 is market-performing).

### 4. AI Evaluation Factors (`ai_evaluator.py`)
The model evaluates four key normalized scores (0-1):
- **Impact Probability**: Likelihood of committee legislation affecting the stock.
- **Lobbying Impact**: Depth of industry lobbying within the relevant committee.
- **Political Factor**: Alignment with current political agendas (e.g., "Trump Interest").
- **Trade Conviction**: Strength of the trade relative to the politician's estimated net worth and historical Alpha.

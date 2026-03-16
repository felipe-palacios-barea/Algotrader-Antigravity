# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 15:35:10 2025

@author: Administrador
"""

import os
from pathlib import Path
import pandas as pd

# === Project Root and .env ===
# Set the root directory for your project files
PROJECT_ROOT = Path(__file__).resolve().parent

# Load environment variables from .env located at project root
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / '.env'
    load_dotenv(dotenv_path=env_path, override=True)
except ImportError:
    pass

# === API Configuration ===
# Pull from environment or fallback to provided key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o-mini")

# === IBKR (Interactive Brokers) Configuration ===
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.getenv("IBKR_PORT", "7497"))  # 7497 for TWS, 7496 for Gateway
IBKR_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "1"))

# === File Paths ===
BASE_DIR = PROJECT_ROOT
TRADES_CSV_PATH = BASE_DIR / "trades.csv"
ENRICHED_TRADES_CSV_PATH = BASE_DIR / "enriched_trades.csv"
FINAL_TRADES_CSV_PATH = BASE_DIR / "final_trades.csv"
EVALUATIONS_CSV_PATH = BASE_DIR / "evaluations.csv"
ORDERS_LOG_CSV_PATH = BASE_DIR / "orders.csv"

# === Trading Settings ===
ORDER_SIZE_USD = float(os.getenv("ORDER_SIZE_USD", "1000"))  # USD per trade
LIKELIHOOD_THRESHOLD = float(os.getenv("LIKELIHOOD_THRESHOLD", "0.5"))  # Threshold for execution

# === Scheduling ===
# Local time HH:MM (Europe/Brussels)
SCHEDULE_TIME = os.getenv("SCHEDULE_TIME", "10:00")

# === Committee URLs ===

# Path to the Excel lookup file
LOOKUP_PATH = Path(__file__).parent / 'look_up.xlsx'

# Load Senate URLs from sheet 'senate_committees', column A (skip header row)
_senate_df = pd.read_excel(
    LOOKUP_PATH,
    sheet_name='senate_committees',
    usecols='A',
    skiprows=0,
    names=['url'],     # explicitly name it "url"
    dtype=str          # ensure they're strings
)
SENATE_COMMITTEE_URLS = _senate_df['url'].dropna().tolist()

# Load House URLs from sheet 'house_committees', column B (skip header row)
_house_df = pd.read_excel(
    LOOKUP_PATH,
    sheet_name='house_committees',
    usecols='B',
    skiprows=1,
    names=['url'],     # explicitly name it "url"
    dtype=str
)
HOUSE_COMMITTEE_URLS = _house_df['url'].dropna().tolist()

# Other config values
REQUEST_TIMEOUT = 10
COMMITTEES_CSV_PATH = Path(__file__).parent / 'committees.csv'

# === HTTP Settings ===
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))  # seconds

# === Political Intelligence Pipeline ===
CONGRESS_GOV_API_KEY = os.getenv("CONGRESS_GOV_API_KEY", "")            # Free from api.data.gov
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "")             # From Meta Developer Portal
POLITICAL_INTELLIGENCE_DB = BASE_DIR / "political_intelligence.db"
MEMBER_HANDLES_CSV = BASE_DIR / "member_handles.csv"
INTELLIGENCE_FETCH_DAYS_BACK = int(os.getenv("INTELLIGENCE_FETCH_DAYS_BACK", "30"))
INTELLIGENCE_PAUSE_SEC = float(os.getenv("INTELLIGENCE_PAUSE_SEC", "2.0"))

import pandas as pd
import yfinance as yf
from pathlib import Path

def get_sp500_symbols():
    # 1) Scrape the S&P 500 tickers from Wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url, attrs={"id": "constituents"})
    df = tables[0]
    return df.Symbol.str.replace(r"\.", "-", regex=True).tolist()

def fetch_and_aggregate(symbols, period="2mo", interval="1d"):
    # 2) Bulk-download all at once
    #    This returns a DataFrame with columns like ('Volume','AAPL'), etc.
    data = yf.download(
        tickers=symbols,
        period=period,
        interval=interval,
        group_by="ticker",
        threads=True,
        progress=False
    )
    # 3) Sum volumes
    records = []
    for sym in symbols:
        try:
            vols = data[sym]["Volume"]
            total = int(vols.sum())
            records.append({"symbol": sym, "total_volume_2m": total})
        except Exception:
            # missing data or delisted → skip
            continue

    return pd.DataFrame(records)

def main():
    # A) Get our universe
    symbols = get_sp500_symbols()

    # B) Fetch & aggregate
    df = fetch_and_aggregate(symbols)

    # C) Sort + top 50
    top50 = df.sort_values("total_volume_2m", ascending=False).head(50)

    # D) Write CSV
    out = Path(__file__).parent / "top50_sp500_2m.csv"
    top50.to_csv(out, index=False)
    print(f"✅ Wrote top 50 by 2-month volume (S&P 500) to {out}")

if __name__ == "__main__":
    main()

import yfinance as yf
import pandas as pd

tickers = [
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN",
    "META", "BRK-B", "TSLA", "XOM", "TSM",
    "JPM", "V", "MA", "WMT", "LLY",
    "UNH", "HD", "BAC", "PG", "CSCO"
]


def safe_get(info, key):
    return info[key] if key in info else None


all_rows = []

for symbol in tickers:
    print(f"\n=== Processing {symbol} ===")

    t = yf.Ticker(symbol)
    info = t.info

    # FORCE Yahoo to include Adj Close
    hist = t.history(
        period="2y",
        interval="1d",
        auto_adjust=False,
        actions=True
    )

    if hist.empty:
        print(f"WARNING: No historical data for {symbol}")
        continue

    hist = hist.reset_index()

    # Now Adj Close WILL exist — detect it
    if "Adj Close" not in hist.columns:
        raise ValueError(f"Adj Close still missing for {symbol}")

    for _, row in hist.iterrows():
        all_rows.append({
            "symbol": symbol,
            "company_name": safe_get(info, "longName"),
            "sector": safe_get(info, "sector"),
            "industry": safe_get(info, "industry"),
            "market_cap": safe_get(info, "marketCap"),
            "beta": safe_get(info, "beta"),
            "dividend_yield": safe_get(info, "dividendYield"),
            "pe_ratio": safe_get(info, "trailingPE"),
            "pb_ratio": safe_get(info, "priceToBook"),
            "ps_ratio": safe_get(info, "priceToSalesTrailing12Months"),

            "date": row["Date"],
            "open": row["Open"],
            "high": row["High"],
            "low": row["Low"],
            "close": row["Close"],
            "volume": row["Volume"],
            "adj_close": row["Adj Close"],   # NOW ALWAYS PRESENT
        })

df = pd.DataFrame(all_rows)
df.to_csv("raw_data.csv", index=False)

print("\n✓ raw_data.csv generated successfully!")
print(f"Total rows: {len(df)}")

import requests, pandas as pd, duckdb, os
from datetime import datetime

BRONZE = "../shared/data/bronze/zar.parquet"
DB     = "../shared/duckdb/de_hero.db"

def run():
    print("Pulling latest ZAR rates...")
    rates = requests.get("https://api.exchangerate-api.com/v4/latest/ZAR").json()["rates"]
    today = {"date": datetime.now().date(), "USD": rates["USD"], "EUR": rates["EUR"], "GBP": rates["GBP"]}

    # Load existing or start fresh
    if os.path.exists(BRONZE):
        df = pd.read_parquet(BRONZE)
        df = pd.concat([df, pd.DataFrame([today])], ignore_index=True)
    else:
        df = pd.DataFrame([today])

    df.to_parquet(BRONZE, index=False)
    print(f"Stored {len(df)} days of ZAR history")

    # DuckDB gold view
    con = duckdb.connect(DB)
    con.execute(f"CREATE OR REPLACE TABLE zar AS SELECT * FROM '{BRONZE}'")
    result = con.execute("""
        SELECT date, USD, EUR, GBP,
               ROUND(USD - LAG(USD) OVER (ORDER BY date), 4) AS usd_change
        FROM zar ORDER BY date DESC LIMIT 10
    """).df()

    print("\nGOLD TABLE – last 10 days:")
    print(result)
    print("\nWEEK 1 100% DONE – screenshot this & post on LinkedIn 🔥")

if __name__ == "__main__":
    run()

"""
Fetch US public holidays for 2022 from date.nager.at and save to CSV,
filtered to Jan–June (to match your 6-month data window).

Run:  python fetch_holidays.py
Out:  data/holidays/us_holidays_2022h1.csv   (columns: holiday_date, holiday_name)

Load that CSV into DIVVY.RAW.HOLIDAYS via the Snowsight "Load Data" wizard,
then `dbt run` — is_holiday will light up for those dates.
"""

import os
import requests
import pandas as pd

YEAR = 2022
COUNTRY = "US"
OUT_DIR = "data/holidays"
OUT_FILE = os.path.join(OUT_DIR, "us_holidays_2022h1.csv")


def fetch_holidays(year, country):
    """Pull all public holidays for the given year + country."""
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()   # list of holiday objects
    # each item has 'date' (YYYY-MM-DD) and 'name'
    return pd.DataFrame([
        {"holiday_date": h["date"], "holiday_name": h["name"]}
        for h in data
    ])


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Fetching {COUNTRY} holidays for {YEAR} ...")
    df = fetch_holidays(YEAR, COUNTRY)
    print(f"  got {len(df)} holidays for the full year")

    # filter to Jan–June (your data window: 2022-01 .. 2022-06)
    df["holiday_date"] = pd.to_datetime(df["holiday_date"])
    df = df[(df["holiday_date"] >= "2022-01-01") &
            (df["holiday_date"] <= "2022-06-30")]
    df["holiday_date"] = df["holiday_date"].dt.strftime("%Y-%m-%d")

    df.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(df)} holidays (Jan–June) -> {OUT_FILE}\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
"""
API ingestion helpers for the Divvy pipeline DAG.

- fetch_weather_to_snowflake: reads the ACTUAL date range of loaded trips from
  Snowflake, then pulls matching historical weather from Open-Meteo. This makes
  weather DYNAMIC: if a new month of trips arrives in S3, the weather fetch
  automatically covers it too, so the trip<->weather join always has data.
- fetch_holidays_to_snowflake: pulls US holidays for the year(s) the trips span
  from date.nager.at.

Credentials come from the dbt profiles.yml mounted at
/home/airflow/.dbt/profiles.yml, so no separate Airflow connection is needed.
Idempotent: each rebuilds its table before loading, so re-runs don't duplicate.
"""

import requests
import yaml
import snowflake.connector

PROFILES_PATH = "/home/airflow/.dbt/profiles.yml"
LAT, LON = 41.88, -87.63


def _sf_connect():
    """Open a Snowflake connection using the dbt profile credentials."""
    with open(PROFILES_PATH) as f:
        profiles = yaml.safe_load(f)
    creds = profiles["divvy_rider_patterns"]["outputs"]["dev"]
    return snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds.get("role"),
        warehouse=creds.get("warehouse"),
        database=creds.get("database", "DIVVY"),
        schema="RAW",
    )


def _trip_date_range(conn):
    """Return (min_date, max_date) of the trips currently in RAW, as ISO strings."""
    cur = conn.cursor()
    cur.execute(
        "SELECT MIN(TRY_TO_TIMESTAMP_NTZ(started_at))::date, "
        "       MAX(TRY_TO_TIMESTAMP_NTZ(started_at))::date "
        "FROM DIVVY.RAW.DIVVY_TRIPS"
    )
    start, end = cur.fetchone()
    if start is None or end is None:
        raise ValueError("No trips found in RAW.DIVVY_TRIPS - load trips before weather.")
    return start.isoformat(), end.isoformat()


def fetch_weather_to_snowflake(**context):
    conn = _sf_connect()
    try:
        # 1. discover the date range of loaded trips
        start, end = _trip_date_range(conn)
        print(f"Trips span {start} to {end} - fetching weather for that range.")

        # 2. fetch weather for EXACTLY that range (dynamic)
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={LAT}&longitude={LON}"
            f"&start_date={start}&end_date={end}"
            "&hourly=temperature_2m,precipitation,wind_speed_10m,apparent_temperature"
            "&timezone=America/Chicago"
        )
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        h = resp.json()["hourly"]

        rows = list(zip(
            h["time"], h["temperature_2m"], h["precipitation"],
            h["wind_speed_10m"], h["apparent_temperature"],
        ))
        print(f"Fetched {len(rows)} hourly weather rows.")

        # 3. rebuild the table (idempotent)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE DIVVY.RAW.WEATHER_HOURLY")
        cur.executemany(
            "INSERT INTO DIVVY.RAW.WEATHER_HOURLY "
            "(weather_time, temperature_2m, precipitation, wind_speed_10m, apparent_temperature) "
            "VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"Loaded {len(rows)} rows into DIVVY.RAW.WEATHER_HOURLY.")
    finally:
        conn.close()


def fetch_holidays_to_snowflake(**context):
    conn = _sf_connect()
    try:
        # match the years the trips span (usually one year, but handle spans)
        start, end = _trip_date_range(conn)
        start_year, end_year = int(start[:4]), int(end[:4])
        years = range(start_year, end_year + 1)

        rows = []
        for year in years:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/US"
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            for hol in resp.json():
                if start <= hol["date"] <= end:      # keep only within trip range
                    rows.append((hol["date"], hol["name"]))
        print(f"Fetched {len(rows)} holidays within {start}..{end}.")

        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE DIVVY.RAW.HOLIDAYS")
        cur.executemany(
            "INSERT INTO DIVVY.RAW.HOLIDAYS (holiday_date, holiday_name) VALUES (%s, %s)",
            rows,
        )
        conn.commit()
        print(f"Loaded {len(rows)} rows into DIVVY.RAW.HOLIDAYS.")
    finally:
        conn.close()
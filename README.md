# DivvyRiderPatterns

An end-to-end data engineering pipeline analyzing how **rider type, weather, and the
calendar** shape demand for Chicago's Divvy bike-share system. Raw trip files land in
AWS S3, weather and holidays are fetched live from public APIs, everything is
transformed in Snowflake with dbt across a medallion architecture, orchestrated by
Apache Airflow in Docker, and visualized in Metabase.

---

## Project Overview

DivvyRiderPatterns is a descriptive analytics pipeline over a fixed six-month window
(January–June 2022). It answers a behavioural question rather than a forecasting one:
**how do members and casual riders differ, and how do weather and the workday calendar
influence when and how much people ride?**

The project demonstrates a full modern data stack — object storage, a cloud warehouse,
templated SQL transformations, workflow orchestration, and a BI dashboard — built and
wired together end-to-end.

## Problem Description

Bike-share operators serve two very different populations: **members** (who tend to
commute) and **casual riders** (who tend to ride for leisure). Understanding how these
groups behave — across the hours of the day, weekdays vs weekends, and under different
weather — is valuable for planning, rebalancing, and messaging.

The analytical questions:
- How does ridership differ between members and casual riders by hour of day?
- How does weather (temperature, precipitation) affect demand?
- How do holidays and weekends change the pattern?
- Do riders respond to weather differently at different parts of the day?

## Key Features

- **Hybrid ingestion** — bulk trip data from S3, reference data (weather, holidays)
  from live APIs, reflecting how each data source actually exists.
- **Dynamic enrichment** — the weather and holiday fetches read the *actual* date range
  of loaded trips and fetch matching data, so the pipeline stays correct if new months
  of trips are added.
- **Medallion architecture** — Bronze (raw) → Silver (cleaned/joined) → Gold (marts).
- **Orchestrated** — a single Airflow trigger runs the whole pipeline, in order, with
  automatic retries.
- **Tested** — dbt data-quality tests run on every pipeline execution.
- **Reproducible** — the whole environment is containerized with Docker.## Datasets

| Source | Data | Origin | Volume |
|---|---|---|---|
| Divvy trips | Individual ride records (Jan–Jun 2022) | AWS S3 (CSV) | ~2.28M rows |
| Weather | Hourly Chicago weather | Open-Meteo Archive API | ~4,344 rows |
| Holidays | US public holidays | date.nager.at API | 8 (within window) |

**Trip fields:** ride id, rideable type, start/end timestamps, start/end stations,
start/end coordinates, member/casual.
**Weather fields:** hourly temperature, precipitation, wind speed, apparent temperature.
**Holiday fields:** date, holiday name.

## Insights

- **Members commute, casual riders don't.** Member ridership shows a classic
  double-peak — a spike around 8am and a larger one around 5pm — with a midday dip.
  Casual ridership rises to a single, smoother afternoon peak. The hour-of-day shape is
  the strongest behavioural differentiator.
- **Casual trips are longer.** Casual riders take noticeably longer trips on average
  than members (leisure vs point-to-point commuting), and the average-vs-median gap is
  wider, indicating more long outlier rides.
- **Casual riding skews to weekends** far more than member riding.
- **Warmer, drier conditions lift demand overall**, with rider-type differences
  persisting across weather.

## Approach

The pipeline was built **complete-but-thin first** — proving the full path end-to-end,
then scaling to all six months. Rider behaviour was anchored on strong, defensible
differentiators (hour-of-day, weekday/weekend, trip duration) rather than weak signals.
The analysis is deliberately descriptive and historical, not predictive.## Tech Stack

| Layer | Technology |
|---|---|
| Object storage (Bronze) | AWS S3 |
| Data warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow 3.3.1 |
| Containerization | Docker / Docker Compose |
| Ingestion | Python (requests, snowflake-connector-python) |
| APIs | Open-Meteo (weather), date.nager.at (holidays) |
| Visualization | Metabase |
| Version control | Git / GitHub |

## Data Pipeline (ELT)

This is an **ELT** pipeline — data is loaded raw first, then transformed in the
warehouse:

1. **Extract & Load**
   - Trips: `COPY INTO` from S3 into `RAW.DIVVY_TRIPS` (via a dbt macro; incremental —
     only new files load).
   - Weather & holidays: Python tasks call the APIs and INSERT into `RAW.WEATHER_HOURLY`
     and `RAW.HOLIDAYS`. Both read the trip date range first, so enrichment always
     matches the trips present.
2. **Transform (dbt, in Snowflake)**
   - **Staging** — clean and type each source; drop broken rows, flag-and-keep valid but
     incomplete rows, dedupe.
   - **Intermediate** — `int_trips_enriched` joins trips to weather (by hour) and
     holidays (by date) with LEFT JOINs, preserving all trips.
   - **Marts (Gold)** — four fact tables for the analytical questions.
3. **Test** — dbt tests validate the outputs.

**Orchestration:** an Airflow DAG runs the whole sequence on one trigger. Trips load
first (so the API fetches can read the date range), weather and holidays fetch in
parallel, then the dbt layers build in order, then tests run. Every task has automatic
retries.
## Data Quality & Testing

Data quality is addressed across the five standard dimensions:

- **Completeness** — `not_null` tests on required fields; trips with missing stations
  are flagged (`station_missing`) and kept, since they remain valid rides.
- **Accuracy** — impossible trips (end ≤ start) are dropped; out-of-region coordinates
  flagged (`geo_bad`); the weather join is verified against real values.
- **Consistency** — `accepted_values` tests constrain `member_casual` and `daypart` to
  valid categories; staging standardizes types.
- **Timeliness** — dynamic API ingestion reads the trip date range and fetches matching
  weather/holidays, verifying alignment rather than trusting a static label.
- **Uniqueness** — `unique` test on `ride_id`; deduplication in staging. A doubled-load
  bug (detected via exact 2× row counts) was diagnosed and fixed with
  truncate-then-single-load.

dbt tests run as the final task on every pipeline execution.

## Analytics

Four Gold marts power the dashboard:

- **`fct_hourly_pattern`** — rides by hour × rider type × weekend (the commute-vs-leisure
  curve).
- **`fct_daily_demand`** — daily rides by rider type with daily weather and
  temperature/precipitation buckets.
- **`fct_rider_summary`** — per-rider-type totals, average and median trip duration, and
  weekend share.
- **`fct_daypart_demand`** — rides and weather by daypart (morning/afternoon/evening),
  capturing within-day weather variation.

Visualized in Metabase, connected directly to the Gold schema.## Scalability & Future Scope

- **New data flows through automatically** — because ingestion is dynamic, adding a new
  month of trips to S3 and re-running the DAG loads the trips and fetches matching
  weather and holidays, with no code changes.
- **Scheduling** — the DAG runs on-demand today (static historical data); a schedule
  could be enabled for continuously arriving data.
- **Potential enhancements:**
  - Snowpipe for event-driven loading as files land in S3.
  - Incremental dbt models to process only new data.
  - Great Expectations as a dedicated data-quality layer with validation reports.
  - CI (GitHub Actions) running `dbt build` on every push.
  - Data-quality alerting on test failures.

---

*Weather data by Open-Meteo (CC BY 4.0). Holiday data by date.nager.at.*

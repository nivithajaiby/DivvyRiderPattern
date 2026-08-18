{#
  Loads raw files from the S3 stage into the RAW tables.
  COPY INTO only loads files Snowflake has NOT already loaded (it tracks
  load history per stage), so re-running is safe and picks up only new files.
  Run with:  dbt run-operation load_raw_from_s3
#}
{% macro load_raw_from_s3() %}

  {% set trips_sql %}
    COPY INTO DIVVY.RAW.DIVVY_TRIPS
    FROM @DIVVY.RAW.divvy_s3_stage/divvytrips/
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';
  {% endset %}

  {% set weather_sql %}
    COPY INTO DIVVY.RAW.WEATHER_HOURLY
    FROM @DIVVY.RAW.divvy_s3_stage/weather/
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';
  {% endset %}

  {% set holidays_sql %}
    COPY INTO DIVVY.RAW.HOLIDAYS
    FROM @DIVVY.RAW.divvy_s3_stage/holidays/
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';
  {% endset %}

  {% do log("Loading trips from S3...", info=True) %}
  {% do run_query(trips_sql) %}

  {% do log("Loading weather from S3...", info=True) %}
  {% do run_query(weather_sql) %}

  {% do log("Loading holidays from S3...", info=True) %}
  {% do run_query(holidays_sql) %}

  {% do log("S3 load complete (only new files were loaded).", info=True) %}

{% endmacro %}

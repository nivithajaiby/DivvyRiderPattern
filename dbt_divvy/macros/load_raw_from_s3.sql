{#
  Loads ONLY trips from the S3 stage into RAW (weather + holidays now come
  from live API calls in the DAG). COPY INTO loads only files not already
  loaded, so re-running is safe and incremental.
  Run with:  dbt run-operation load_trips_from_s3
#}
{% macro load_trips_from_s3() %}

  {% set trips_sql %}
    COPY INTO DIVVY.RAW.DIVVY_TRIPS
    FROM @DIVVY.RAW.divvy_s3_stage/divvytrips/
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';
  {% endset %}

  {% do log("Loading trips from S3...", info=True) %}
  {% do run_query(trips_sql) %}
  {% do log("Trips load complete (only new files were loaded).", info=True) %}

{% endmacro %}

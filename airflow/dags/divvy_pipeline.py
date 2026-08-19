from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/dags")
from api_ingest import fetch_weather_to_snowflake, fetch_holidays_to_snowflake

DBT_DIR = "/opt/dbt_divvy"
PROFILES_DIR = "/home/airflow/.dbt"

default_args = {
    "owner": "nivitha",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def dbt_run_task(task_id, select=None):
    cmd = f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR}"
    if select:
        cmd += f" --select {select}"
    return BashOperator(task_id=task_id, bash_command=cmd)

with DAG(
    dag_id="divvy_pipeline",
    description="Divvy: trips from S3, then dynamic weather/holidays via API -> medallion -> test",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["divvy", "dbt", "snowflake", "api"],
) as dag:

    # 1. trips must load FIRST - weather/holidays read the trip date range
    load_trips_s3 = BashOperator(
        task_id="load_trips_s3",
        bash_command=f"cd {DBT_DIR} && dbt run-operation load_trips_from_s3 --profiles-dir {PROFILES_DIR}",
    )

    # 2. weather + holidays fetch dynamically, matching the trip range (run in parallel, after trips)
    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_to_snowflake,
    )
    fetch_holidays = PythonOperator(
        task_id="fetch_holidays",
        python_callable=fetch_holidays_to_snowflake,
    )

    # 3. transformation
    staging = dbt_run_task("staging", select="staging")
    intermediate = dbt_run_task("intermediate", select="intermediate")
    marts = dbt_run_task("marts", select="marts")
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR}",
    )

    # dependencies: trips -> (weather + holidays) -> staging -> intermediate -> marts -> test
    load_trips_s3 >> [fetch_weather, fetch_holidays] >> staging >> intermediate >> marts >> dbt_test

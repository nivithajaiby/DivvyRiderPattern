from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_DIR = "/opt/dbt_divvy"
PROFILES_DIR = "/home/airflow/.dbt"

default_args = {
    "owner": "nivitha",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="divvy_pipeline",
    description="DivvyRiderPatterns: build and test dbt models on Snowflake",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["divvy", "dbt", "snowflake"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR}",
    )

    dbt_run >> dbt_test

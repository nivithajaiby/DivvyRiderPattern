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

def dbt_run_task(task_id, select=None):
    cmd = f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR}"
    if select:
        cmd += f" --select {select}"
    return BashOperator(task_id=task_id, bash_command=cmd)

with DAG(
    dag_id="divvy_pipeline",
    description="DivvyRiderPatterns: S3 load -> medallion (staging -> intermediate -> marts) -> test",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["divvy", "dbt", "snowflake"],
) as dag:

    load_from_s3 = BashOperator(
        task_id="load_from_s3",
        bash_command=f"cd {DBT_DIR} && dbt run-operation load_raw_from_s3 --profiles-dir {PROFILES_DIR}",
    )

    staging = dbt_run_task("staging", select="staging")
    intermediate = dbt_run_task("intermediate", select="intermediate")
    marts = dbt_run_task("marts", select="marts")

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR}",
    )

    load_from_s3 >> staging >> intermediate >> marts >> dbt_test

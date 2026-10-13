# DAG: run the dbt project in stages, testing after every stage.
# If a test with severity "error" fails, the next stage does not start.
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
PROJECT_DIR = "/opt/airflow/dbt/supply_pulse"
TARGET = "prod"

default_args = {"retries": 0}


def dbt_task(task_id, command):
    # Every task runs one dbt command inside the project folder
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {DBT} {command} --target {TARGET}",
        # dbt writes logs and compiled files to /tmp (the mounted folder is read-heavy)
        env={"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"},
        append_env=True,
    )


with DAG(
    dag_id="supply_pulse_dbt_batch",
    start_date=datetime(2026, 10, 1),
    schedule=None,  # manual trigger for now; a schedule is added later
    catchup=False,
    default_args=default_args,
    tags=["dbt", "supply_pulse"],
) as dag:

    deps = dbt_task("dbt_deps", "deps")
    seeds = dbt_task("dbt_seed", "seed")

    # Stage 1: staging (SILVER views)
    stg_run = dbt_task("staging_run", "run --select path:models/staging")
    stg_test = dbt_task("staging_test", "test --select path:models/staging")

    # Stage 2: intermediate (SILVER views)
    int_run = dbt_task("intermediate_run", "run --select path:models/intermediate")
    int_test = dbt_task("intermediate_test", "test --select path:models/intermediate")

    # Stage 3: dimensions (GOLD tables)
    dim_run = dbt_task("dimensions_run", "run --select path:models/marts/dimensions")
    dim_test = dbt_task("dimensions_test", "test --select path:models/marts/dimensions")

    # Stage 4: facts (GOLD tables); their tests need the dimensions built above
    fact_run = dbt_task("facts_run", "run --select path:models/marts/facts")
    fact_test = dbt_task("facts_test", "test --select path:models/marts/facts")

    (
        deps >> seeds
        >> stg_run >> stg_test
        >> int_run >> int_test
        >> dim_run >> dim_test
        >> fact_run >> fact_test
    )
# DAG 1 - BATCH: BRONZE -> SILVER -> GOLD with dbt, with a RECOVERY PLAN.
#
#   bronze_ready -> backup_gold -> dbt_deps -> dbt_seed -> staging (run, test) -> intermediate (run, test)
#                -> dimensions (run, test) -> facts (run, test)
#
# Recovery plan (why GOLD is never left half-built):
#   1. backup_gold   zero-copy clone  GOLD -> GOLD_BACKUP  (instant, costs no storage until GOLD changes)
#   2. any dbt task fails (a run, or a test with severity "error")
#        -> the chain stops: later stages never build on bad data
#        -> restore_gold swaps GOLD_BACKUP back, so GOLD is the last GOOD version again
#        -> final_verdict marks the whole run FAILED (and Slack is notified) even though restore worked
#   3. transient errors (network, warehouse resuming) are retried twice for run/seed/deps tasks.
#      Tests are never retried: a failed test is a data problem, not a glitch.
#   4. Re-running is always safe: dbt rebuilds every model from the full RAW_EVENTS history
#      (idempotent), so "Clear" on a failed task in the Airflow UI is a valid manual recovery.
#
# Known limit: while the run is busy, GOLD is rebuilt table by table, so a reader (Power BI) can see a mix of
# old and new tables for a few minutes. Hiding that completely needs a blue/green schema swap (not built).
import os
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState

VENV = "/home/airflow/dbt_venv/bin"          # dbt and the Snowflake connector live in this virtualenv
PROJECT_DIR = "/opt/airflow/dbt/supply_pulse"
TARGET = "prod"                              # reads the BRONZE views, builds SILVER / GOLD (seeds in PROD)


def notify_slack(context):
    # DAG-level callback: ONE message per failed run (never write the webhook URL in code)
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    dag_run = context["dag_run"]
    failed = [ti.task_id for ti in dag_run.get_task_instances() if ti.state == TaskInstanceState.FAILED]
    text = f"Airflow: batch run {dag_run.run_id} FAILED at {failed}. GOLD was restored to the last good version."
    try:
        requests.post(url, json={"text": text}, timeout=10)
    except requests.RequestException:
        pass  # a failed alert must never hide the original failure


def dbt_task(task_id, command, retries=0):
    # Every task runs one dbt command inside the project folder
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {VENV}/dbt {command} --target {TARGET}",
        # dbt writes logs and compiled files to /tmp, not into the mounted project folder
        env={"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"},
        append_env=True,
        retries=retries,                      # only for glitches: tests keep retries=0
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=45),   # a hung query must not block tomorrow's run
    )


# Python snippet that runs inside the dbt virtualenv (it has the Snowflake connector)
SF_CONNECT = """
import os, sys
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.environ["SHARED_SNOWFLAKE_ACCOUNT"], user=os.environ["SHARED_SNOWFLAKE_USER"],
    password=os.environ["SHARED_SNOWFLAKE_PASSWORD"], role=os.environ.get("SHARED_SNOWFLAKE_ROLE"),
    warehouse=os.environ.get("SHARED_SNOWFLAKE_WAREHOUSE"),
    database=os.environ.get("SHARED_SNOWFLAKE_DATABASE", "SUPPLY_PULSE_DB"))
cur = conn.cursor()
"""


def sf_bash(body):
    return f"{VENV}/python - <<'PYEOF'\n{SF_CONNECT}{body}\nconn.close()\nPYEOF\n"


# Fails fast, with a clear message, when the streaming side has not delivered anything to BRONZE yet
BRONZE_READY = sf_bash("""
cur.execute("select count(*) from BRONZE.RAW_EVENTS")
events = cur.fetchone()[0]
cur.execute("select count(*) from information_schema.views where table_schema = 'BRONZE'")
views = cur.fetchone()[0]
print("RAW_EVENTS rows:", events, "| BRONZE views:", views)
if events == 0:
    sys.exit("RAW_EVENTS is empty: start the bronze-loader (profile snowflake) first")
if views == 0:
    sys.exit("No BRONZE views: run  python scripts/create_bronze_views.py")
""")

# The LAST printed line becomes the task's XCom value: restore_gold reads it to know a backup exists
BACKUP_GOLD = sf_bash("""
cur.execute("select count(*) from information_schema.schemata where schema_name = 'GOLD'")
if cur.fetchone()[0] == 0:
    print("GOLD does not exist yet (first run): nothing to back up")
    print("BACKUP_SKIPPED")
else:
    cur.execute("create or replace schema GOLD_BACKUP clone GOLD")
    print("GOLD cloned to GOLD_BACKUP (zero-copy)")
    print("BACKUP_OK")
""")

# Exit code 99 = "skip" for BashOperator. Restore ONLY if THIS run made a backup, otherwise we would
# roll GOLD back to an old backup from a previous night.
RESTORE_GOLD = (
    """if [ "{{ ti.xcom_pull(task_ids='backup_gold') }}" != "BACKUP_OK" ]; then
  echo "No backup was made in this run: GOLD is left as it is"; exit 99
fi
"""
    + sf_bash("""
cur.execute("alter schema GOLD swap with GOLD_BACKUP")
print("GOLD swapped with GOLD_BACKUP: the last good GOLD is live again")
print("The failed build is kept in GOLD_BACKUP for inspection; the next run replaces it")
""")
)


def final_verdict(**context):
    # restore_gold succeeding must not turn a failed run into a green one: this task decides the run state
    dag_run = context["dag_run"]
    skip = {"restore_gold", "final_verdict"}
    bad = [ti.task_id for ti in dag_run.get_task_instances()
           if ti.task_id not in skip
           and ti.state in (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED)]
    if bad:
        raise AirflowFailException(f"Batch failed in {bad}. GOLD was restored (see restore_gold).")
    print("All batch tasks succeeded")


with DAG(
    dag_id="supply_pulse_batch",
    description="BRONZE -> SILVER -> GOLD with dbt, tests after every stage, backup and restore of GOLD",
    # time zone-aware start date: the schedule below is read in Cairo time
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Cairo"),
    schedule="0 3 * * *",        # every day at 03:00 Cairo time (None = manual trigger only)
    catchup=False,
    max_active_runs=1,           # never two runs at the same time
    default_args={"retries": 0},
    on_failure_callback=notify_slack,
    tags=["batch", "dbt", "supply_pulse"],
) as dag:

    bronze_ready = BashOperator(task_id="bronze_ready", bash_command=BRONZE_READY,
                                retries=2, retry_delay=timedelta(minutes=2))
    backup_gold = BashOperator(task_id="backup_gold", bash_command=BACKUP_GOLD,
                               retries=2, retry_delay=timedelta(minutes=2))

    deps = dbt_task("dbt_deps", "deps", retries=2)
    seeds = dbt_task("dbt_seed", "seed", retries=2)

    # Stage 1: staging (SILVER views)
    stg_run = dbt_task("staging_run", "run --select path:models/staging", retries=2)
    stg_test = dbt_task("staging_test", "test --select path:models/staging --indirect-selection buildable")

    # Stage 2: intermediate (SILVER views)
    int_run = dbt_task("intermediate_run", "run --select path:models/intermediate", retries=2)
    int_test = dbt_task("intermediate_test", "test --select path:models/intermediate --indirect-selection buildable")

    # Stage 3: dimensions (GOLD tables)
    dim_run = dbt_task("dimensions_run", "run --select path:models/marts/dimensions", retries=2)
    dim_test = dbt_task("dimensions_test", "test --select path:models/marts/dimensions --indirect-selection buildable")

    # Stage 4: facts (GOLD tables); their tests need the dimensions built above
    fact_run = dbt_task("facts_run", "run --select path:models/marts/facts", retries=2)
    fact_test = dbt_task("facts_test", "test --select path:models/marts/facts --indirect-selection buildable")

    restore_gold = BashOperator(task_id="restore_gold", bash_command=RESTORE_GOLD,
                                trigger_rule="one_failed")       # runs when ANY parent failed
    verdict = PythonOperator(task_id="final_verdict", python_callable=final_verdict,
                             trigger_rule="all_done")            # always runs, decides the run state

    chain = [bronze_ready, backup_gold, deps, seeds, stg_run, stg_test, int_run, int_test,
             dim_run, dim_test, fact_run, fact_test]
    for upstream, downstream in zip(chain, chain[1:]):
        upstream >> downstream

    chain >> restore_gold            # every task of the chain is a parent of restore_gold
    [fact_test, restore_gold] >> verdict
    
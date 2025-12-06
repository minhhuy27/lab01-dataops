"""DAG orchestration for DBT: bronze -> silver -> gold with tests."""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_ROOT = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"
DEFAULT_PATH = ":".join(
    [
        "/home/airflow/.local/bin",
        "/usr/local/sbin",
        "/usr/local/bin",
        "/usr/sbin",
        "/usr/bin",
        "/sbin",
        "/bin",
    ]
)
DBT_PACKAGES_PATH = "/tmp/dbt_packages_cache"
DBT_TARGET_PATH = "/tmp/dbt_target"
DEFAULT_ENV = {
    "DBT_PROFILES_DIR": DBT_ROOT,
    "PATH": DEFAULT_PATH,
    "DBT_LOG_PATH": "/tmp/dbt_logs",
    "DBT_PACKAGES_INSTALL_PATH": DBT_PACKAGES_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
}
CMD_PREFIX = (
    f"mkdir -p /tmp/dbt_logs {DBT_PACKAGES_PATH} {DBT_TARGET_PATH} && "
    f"cd {DBT_ROOT} && "
)


def notify_failure(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    logging.error(
        "DBT DAG failed at task=%s, dag=%s, execution=%s",
        task_id,
        dag_id,
        execution_date,
    )


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


doc_md = """
## Purpose
- Run DBT by layer: bronze -> silver -> gold.
- Validate data with dbt test.
- Include retry and error logging via callback.

## Defaults
- Schedule: `0 0 * * *` (daily, no catchup).
- DBT runs inside Airflow container at `/opt/airflow/dbt`.
"""

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Orchestrate DBT models by layer and run tests",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "sqlserver", "dataops"],
    doc_md=doc_md,
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=CMD_PREFIX
        + f"DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_PATH} {DBT_BIN} deps",
        env=DEFAULT_ENV,
    )

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=CMD_PREFIX
        + f"DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_PATH} {DBT_BIN} "
        + "run --select path:models/bronze",
        env=DEFAULT_ENV,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=CMD_PREFIX
        + f"DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_PATH} {DBT_BIN} "
        + "run --select path:models/silver",
        env=DEFAULT_ENV,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=CMD_PREFIX
        + f"DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_PATH} {DBT_BIN} "
        + "run --select path:models/gold",
        env=DEFAULT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=CMD_PREFIX
        + f"DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_PATH} {DBT_BIN} test",
        env=DEFAULT_ENV,
    )

    dbt_deps >> dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_test

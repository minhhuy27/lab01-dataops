"""
DAG điều phối DBT: chạy tuần tự bronze -> silver -> gold và kiểm thử.
Lịch: hàng ngày (0h), có retry và callback khi lỗi.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_ROOT = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"
DEFAULT_PATH = "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
DEFAULT_ENV = {
    "DBT_PROFILES_DIR": DBT_ROOT,
    "PATH": DEFAULT_PATH,
    "DBT_LOG_PATH": "/tmp/dbt_logs",
    "DBT_PACKAGES_INSTALL_PATH": "/tmp/dbt_packages",
    "DBT_TARGET_PATH": "/tmp/dbt_target",
}


def notify_failure(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    logging.error("DBT DAG thất bại tại task=%s, dag=%s, execution=%s", task_id, dag_id, execution_date)


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
## Mục tiêu
- Chạy DBT theo thứ tự layer: bronze → silver → gold.
- Kiểm thử dữ liệu sau khi chạy (dbt test).
- Có retry, log lỗi qua callback.

## Mặc định
- Lịch: `0 0 * * *` (hàng ngày, không catchup).
- DBT chạy trong container Airflow tại `/opt/airflow/dbt`.
"""

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Orchestrate DBT models theo layer và kiểm thử dữ liệu",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "sqlserver", "dataops"],
    doc_md=doc_md,
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"mkdir -p /tmp/dbt_logs /tmp/dbt_packages /tmp/dbt_target && "
            f"cd {DBT_ROOT} && "
            f"DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages {DBT_BIN} deps"
        ),
        env=DEFAULT_ENV,
    )

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=(
            f"mkdir -p /tmp/dbt_logs /tmp/dbt_packages /tmp/dbt_target && "
            f"cd {DBT_ROOT} && "
            f"DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages {DBT_BIN} run --select path:models/bronze"
        ),
        env=DEFAULT_ENV,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"mkdir -p /tmp/dbt_logs /tmp/dbt_packages /tmp/dbt_target && "
            f"cd {DBT_ROOT} && "
            f"DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages {DBT_BIN} run --select path:models/silver"
        ),
        env=DEFAULT_ENV,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"mkdir -p /tmp/dbt_logs /tmp/dbt_packages /tmp/dbt_target && "
            f"cd {DBT_ROOT} && "
            f"DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages {DBT_BIN} run --select path:models/gold"
        ),
        env=DEFAULT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"mkdir -p /tmp/dbt_logs /tmp/dbt_packages /tmp/dbt_target && "
            f"cd {DBT_ROOT} && "
            f"DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages {DBT_BIN} test"
        ),
        env=DEFAULT_ENV,
    )

    dbt_deps >> dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_test

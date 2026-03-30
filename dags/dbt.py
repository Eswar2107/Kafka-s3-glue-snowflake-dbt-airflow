from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PATH = "/home/airflow/.local/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/faker_store"

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt"]
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        cd {DBT_PROJECT} && {DBT_PATH} deps
        """
    )

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"""
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        cd {DBT_PROJECT} && {DBT_PATH} run --select bronze
        """
    )

    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=f"""
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        cd {DBT_PROJECT} && {DBT_PATH} snapshot
        """
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"""
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        cd {DBT_PROJECT} && {DBT_PATH} run --select silver
        """
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"""
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        cd {DBT_PROJECT} && {DBT_PATH} build --select gold --fail-fast
        """
    )

    dbt_deps >> bronze >> snapshot >> silver >> gold
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="airflow_timezone_and_health_test",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["testing"],
) as dag:

    check_date_task = BashOperator(
        task_id="print_current_date_and_time",
        bash_command="date",
    )

    check_date_task
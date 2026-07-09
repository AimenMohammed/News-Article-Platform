from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Production reliability and error-handling configurations
production_args = {
    'owner': 'Aimen Al-Faqih',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,  # Retry up to 3 times in case of transient network or resource errors
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'iceberg_production_maintenance_dag',
    default_args=production_args,
    description='Production grade Apache Iceberg table maintenance and compaction',
    schedule_interval='0 2 * * 0',  # Scheduled every Sunday at 2:00 AM (off-peak hours)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent overlapping maintenance runs to avoid resource contention
) as dag:

    production_iceberg_compaction = SparkSubmitOperator(
        task_id='prod_rewrite_iceberg_data_files',
        application='/opt/airflow/dags/scripts/iceberg_maintenance.py',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
        },
        dag=dag,
    )

    production_iceberg_compaction
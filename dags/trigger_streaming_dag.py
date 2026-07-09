from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# إعدادات إعادة المحاولة في حال الفشل
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,                        # ◄ عدد مرات إعادة المحاولة في حال حدوث خطأ
    'retry_delay': timedelta(minutes=2), # ◄ وقت الانتظار قبل إعادة المحاولة (دقيقة واحدة)
}

with DAG(
    '1_streaming_trigger_dag',
    default_args=default_args,
    description='Automated Micro-batch pipeline running every 5 minutes',
    schedule_interval=timedelta(minutes=5), # ◄ تشغيل تلقائي متواصل كل 5 دقائق
    start_date=datetime(2026, 7, 7),
    catchup=False,
    max_active_runs=1,                      # يمنع تداخل الدورات إذا استغرقت إحداها أكثر من 5 دقائق
) as dag:

    # 1. طبقة البرونز (Bronze Ingestion)
    run_bronze_ingestion = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/bronze_ingestion.py',
    )
    
    # 2. طبقة الفضة (Silver Transformation)
    run_silver_transform = BashOperator(
        task_id='run_silver_transformation',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/silver_transformation.py',
    )
    

    # تسلسل التدفق المستقر
    run_bronze_ingestion >> run_silver_transform
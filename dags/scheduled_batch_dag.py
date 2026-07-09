from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# إعدادات إعادة المحاولة المحدثة (إعادة المحاولة بعد دقيقتين)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,                        # ◄ عدد مرات إعادة المحاولة في حال حدوث خطأ
    'retry_delay': timedelta(minutes=2), # ◄ وقت الانتظار قبل إعادة المحاولة (دقيقتين)
}

with DAG(
    '2_scheduled_batch_dag',
    default_args=default_args,
    description='Scheduled Batch DAG for Medallion Architecture layers',
    schedule_interval=timedelta(minutes=15),  # ◄ تم التعديل ليعمل دورياً كل 15 دقيقة
    start_date=datetime(2026, 7, 7),
    catchup=False,
    max_active_runs=1,                        # يمنع تداخل المهام والدورات في حال التأخير
) as dag:

    # أ. جلب البيانات بالـ Scraping
    run_scrape = BashOperator(
        task_id='run_scrape_news',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/scrape_news.py',
    )
    
    # ب. تشغيل سكربت استهلاك الـ Streaming / Ingestion
    run_ingest_batch = BashOperator(
        task_id='run_ingest_news_batch',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/ingest_news_batch.py',
    )
    

   

    # تسلسل وتنفيذ مهام الباتش
    run_scrape >> run_ingest_batch 
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
    '3_gold_layer_update',
    default_args=default_args,
    description='gold_layer_update DAG for Medallion Architecture layers',
    schedule_interval=timedelta(minutes=15),  # ◄ تم التعديل ليعمل دورياً كل 15 دقيقة
    start_date=datetime(2026, 7, 7),
    catchup=False,
    max_active_runs=1,                        # يمنع تداخل المهام والدورات في حال التأخير
) as dag:

    # 1. طبقة الذهب (Gold Transformation)
    run_gold = BashOperator(
        task_id='run_gold_transformation',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/gold_transformation.py',
    )

    # 2. مهمة ترحيل البيانات إلى ClickHouse (تمت إضافتها هنا)
    run_clickhouse_migration = BashOperator(
        task_id='run_gold_to_clickhouse',
        bash_command='docker exec jupyter-spark spark-submit /home/jovyan/work/gold_to_clickhouse.py',
    )

    # ◄ تسلسل وتنفيذ المهام بالترتيب المنطقي
    run_gold >> run_clickhouse_migration
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='data_crawling',
    schedule='0 0 * * 1',   # ← đổi ở đây
    start_date=pendulum.now("Asia/Ho_Chi_Minh").subtract(days=1),
    catchup=False
):
    crawl_from_top_cv = BashOperator(
        task_id='crawl_from_top_cv',
        bash_command='python /opt/airflow/tasks/crawl_from_top_cv.py'
    )
    crawl_from_careerviet = BashOperator(
        task_id='crawl_from_careerviet',
        bash_command='python /opt/airflow/tasks/crawl_from_careerviet3.py'
    )

[crawl_from_top_cv, crawl_from_careerviet]
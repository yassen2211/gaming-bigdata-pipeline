from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = dict(owner="airflow", retries=1, retry_delay=timedelta(minutes=2))

with DAG(dag_id="gaming_pipeline", default_args=default_args, schedule_interval=None, start_date=datetime(2026,1,1), catchup=False, tags=["gaming","etl","spark","snowflake"]) as dag:
    t1 = BashOperator(task_id="upload_to_hdfs", bash_command="echo Upload_done")
    t2 = BashOperator(task_id="spark_extract", bash_command="echo Extract_done")
    t3 = BashOperator(task_id="spark_transform", bash_command="echo Transform_done")
    t4 = BashOperator(task_id="load_to_snowflake", bash_command="echo Load_done")
    t1 >> t2 >> t3 >> t4

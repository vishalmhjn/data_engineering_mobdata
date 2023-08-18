import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from traffic_data.madrid_real_time import get_feed

default_args = {
    "owner": "vishal",
    "start_date": dt.datetime(
        2023, 8, 18, 13, 25, 0, tzinfo=dt.timezone(dt.timedelta(seconds=7200))
    ),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "MadridData",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
) as dag:
    print_starting = BashOperator(
        task_id="starting",
        bash_command='echo "Starting now....."',
    )
    csvJson = PythonOperator(
        task_id="madriddatacollect",
        python_callable=get_feed,
    )
print_starting >> csvJson

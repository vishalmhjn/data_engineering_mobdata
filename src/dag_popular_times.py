import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from popular_times.get_popular_times import wrapper

default_args = {
    "owner": "vishal",
    "start_date": dt.datetime(
        2023, 8, 18, 13, 0, 0, tzinfo=dt.timezone(dt.timedelta(seconds=7200))
    ),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=60),
}

with DAG(
    "GooglePopularTimes",
    default_args=default_args,
    schedule_interval=timedelta(minutes=60),
) as dag:
    print_starting = BashOperator(
        task_id="starting",
        bash_command='echo "I am downloading popular times now....."',
    )
    csvJson = PythonOperator(task_id="obtainPopularTimes", python_callable=wrapper)
print_starting >> csvJson

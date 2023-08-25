import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from popular_times.main_wrapper_poi import main

# Define default arguments for the DAG
default_args = {
    "owner": "vishal",
    "start_date": dt.datetime(
        2023, 8, 24, 16, 30, 0, tzinfo=dt.timezone(dt.timedelta(seconds=7200))
    ),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=10),
}

# Create the DAG
with DAG(
    "GooglePopularTimes_POIs",
    default_args=default_args,
    schedule_interval=timedelta(minutes=60),
) as dag:
    # Task to print a starting message
    print_starting = BashOperator(
        task_id="startingPOI",
        bash_command='echo "I am downloading and saving popular times now....."',
    )

    # Task to obtain POI popular times using Python
    csvJson = PythonOperator(
        task_id="obtainPOIPopularTimes",
        python_callable=main,
    )

# Define task dependencies
print_starting >> csvJson

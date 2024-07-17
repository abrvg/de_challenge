from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configure DAG name and schedule
default_args = {
    'owner': 'a',
    'start_date': datetime(2024, 7, 13),  # Starts tomorrow
    'email': ['airflow@airflow.com'],
    'email_on_failure' : 'False',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id='daily_etl',
    default_args=default_args,
    schedule='@daily',
) as dag:

    # Task to run etl.py
    t1 = BashOperator(
        task_id='run_etl_py',
        bash_command = 'python /GitHub/ch_2/etl.py',  # Execute the python script
        
    )

t1

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

# 1. Define the logic for your tasks
def get_random_number():
    number = random.randint(1, 100)
    print(f"Generated number: {number}")
    return number

def process_data(ti):
    # 'ti' is the Task Instance, used to pull data from a previous task (XCom)
    value = ti.xcom_pull(task_ids='generate_number')
    result = value * 2
    print(f"The double of {value} is {result}")
    return result

def print_context(**context):
    # Accessing Airflow metadata (logical date, dag_run, etc.)
    execution_date = context['logical_date']
    print(f"This task is running for the logical date: {execution_date}")

# 2. Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        dag_id='python_operator_demo',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:

    # 3. Create the tasks
    t1 = PythonOperator(
        task_id='generate_number',
        python_callable=get_random_number
    )

    t2 = PythonOperator(
        task_id='process_calculated_data',
        python_callable=process_data
    )

    t3 = PythonOperator(
        task_id='log_airflow_context',
        python_callable=print_context,
        provide_context=True # Modern Airflow (2.0+) does this automatically
    )

    # 4. Set dependencies
    t1 >> t2 >> t3
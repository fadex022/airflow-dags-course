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
    if value is None:
        value = 0
    result = value * 2
    print(f"The double of {value} is {result}")
    return result

def print_context(**context):
    # Modern Airflow passes the context automatically
    # 'logical_date' is the preferred term over 'execution_date'
    log_date = context.get('logical_date')
    print(f"This task is running for the logical date: {log_date}")

# 2. Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'python_operator_demo',
    default_args=default_args,
    description='DAG utilisant des opérateurs Python',
    schedule='0 0 * * *',
    catchup=False,
)

# 3. Create the tasks
t1 = PythonOperator(
    task_id='generate_number',
    python_callable=get_random_number,
    dag=dag
)

t2 = PythonOperator(
    task_id='process_calculated_data',
    python_callable=process_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='log_airflow_context',
    python_callable=print_context,
    # REMOVED: provide_context=True (it's now automatic)
    dag=dag
)

# 4. Set dependencies
t1 >> t2 >> t3
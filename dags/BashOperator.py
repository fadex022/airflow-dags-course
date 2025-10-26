from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'exemple_bash',
    default_args=default_args,
    description='DAG utilisant des opérateurs Bash',
    schedule='0 0 * * *',
    catchup=False,
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag,
)

t3 = BashOperator(
    task_id='echo',
    bash_command='echo "Tâche terminée!"',
    dag=dag,
)

t1 >> t2 >> t3
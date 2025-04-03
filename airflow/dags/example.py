from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def first_task(**kwargs):
    text = 'Text to transfer'
    kwargs['ti'].xcom_push(key='text_to_task2', value = text)

def second_task(**kwargs):
    text = kwargs['ti'].xcom_pull(key='text_to_task2', task_ids = 'share_with_xcom')
    print(text)


default_args = {
    'start_date' : datetime(2025, 4, 3)
}

with DAG(
    dag_id= 'Example_with_kwargs_xcom',
    default_args = default_args,
    schedule_interval= '@daily',
    catchup= False
) as dag:
    task_first = PythonOperator (
        task_id = "share_with_xcom",
        python_callable=first_task,
        provide_context = True
    )

    second_dag_task = PythonOperator(
        task_id = "take_from_xcom",
        provide_context = True,
        python_callable=second_task
    )
    task_first >> second_dag_task
    
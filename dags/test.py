from datetime import datetime, timedelta

import pendulum

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


args = {
    'owner': 'denis',
    'start_date': datetime(2023, 3, 10, tzinfo=pendulum.timezone('Europe/Moscow')),
    'catchup': True,
    'retries': 3,
    'retry_delay': timedelta(hours=1),
    'max_active_runs': 1
}


def print_something() -> None:
    """
    Печатает текст.

    :return: `None`
    """
    print('something')

with DAG(
        dag_id='testdag1',
        schedule_interval='10 0 * * *',
        default_args=args,
        tags=['testdag1', 'test'],
        description='',
        concurrency=1
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    print_something = PythonOperator(
        task_id='print_something',
        python_callable=print_something,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> print_something >> end
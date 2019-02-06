import datetime as dt
from airflow import DAG

from airflow.operators.bash_operator import BashOperator

default_args = {
        'owner': 'airflow',
        'start_date': dt.datetime.now(),
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=3)
        }

with DAG('database_task',
        default_args=default_args,
        schedule_interval='*/15 * * * *',
        ) as dag:


    load_database = BashOperator(task_id='load_database',
                                bash_command='python ~/Insight-GDELT-Feed/gdelt/new_event_collection.py')

    db_update_confirm = BashOperator(task_id='db_update_confirm',
                                bash_command="echo 'Hey, I added new events to database'")

load_database >> db_update_confirm

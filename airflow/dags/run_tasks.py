import datetime as dt
from airflow import DAG
import airflow
from airflow.operators.bash_operator import BashOperator

default_args = {
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2),
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=3)
        }

dag = DAG('database_task',
        default_args=default_args,
        schedule_interval='*/15 * * * *',
        )

db_update_confirm = BashOperator(task_id='db_update_confirm',
                                bash_command="echo 'Hey, I added new events to database'",
                                dag=dag)

load_database = BashOperator(task_id='load_database',
                                bash_command='python ~/Insight-GDELT-Feed/gdelt/new_event_collection.py',
                                dag=dag)


load_database >> db_update_confirm

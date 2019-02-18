import datetime as dt
from airflow import DAG
import airflow
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 02, 18), #airflow.utils.dates.days_ago(2),
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
                                bash_command='python ~/Insight-GDELT-Feed/src/gdelt/event_updater.py',
                                dag=dag)


load_database >> db_update_confirm

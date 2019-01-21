from __future__ import print_function
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
}

dag = DAG(
    dag_id='dag_experiment', default_args=args,
    schedule_interval=None)

def print_context(i):
    print(i)
    return 'print_context has sucess {}'.format(i)



parent = None
for i in range(10):
    '''
    Generating 10 sleeping task, sleeping from 0 to 9 seconds
    respectively
    '''
    task = \
        PythonOperator(
            task_id='print_the_context.{}'.format(i),
            python_callable=print_context,
            op_kwargs={'i': i},
            dag=dag)

    if parent:
        task.set_upstream(parent)

    parent = task
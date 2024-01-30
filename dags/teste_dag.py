from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
        'owner':'Alison',
        'start_date':datetime(2023, 7, 19),
        'end_date':datetime(2023, 7, 20),
        'retries': 4,
        'retry_delay': timedelta(seconds=120),
        'wait_for_downstream': False,
        'depends_on_past': True,
    }

with DAG(
    dag_id='teste-dag-composer',
    default_args= default_args,
    schedule_interval= '00 08 1-31 1-12 *',
    catchup= True,
    tags= ['teste'],
) as dag:
    
    dag_init = DummyOperator(task_id='dag_init', dag=dag)
    dag_end  = DummyOperator(task_id='dag_end', dag=dag)

    dag_init >> dag_end
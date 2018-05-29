from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators import HttpSensor


default_args = {
    'owner': 'sindu',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 29),
    'email': ['sindukandula@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}


def get_date(ds, **kwargs):
    daten = datetime.now()
    minz=str(daten.minute)
    hr=str(daten.hour)
    mon=str(daten.month)
    if daten.second >= 0:
        sec="00"
    if daten.minute >=0 and daten.minute<=14 :
        minz="00"
    elif daten.minute >=15 and daten.minute<=29 :
        minz="15"
    elif daten.minute >=30 and daten.minute<=44 :
        minz="30"
    elif daten.minute >=45 and daten.minute<=59 :
        minz="45"
    if daten.hour <= 9:
        hr="0"+hr
    if daten.month<=9:
        mon="0"+mon    
    daten=str(daten.year)+mon+str(daten.day)+hr+minz+sec
    print (daten)
    return daten

dag = DAG(
    'events_data', default_args=default_args, schedule_interval = '*/15 * * * *')

# t1, t2 and t3 are examples of tasks created by instantiating operators


t1 = PythonOperator(
    task_id='print_date',
    provide_context=True, 
    python_callable=get_date,
    dag=dag)

sensor = HttpSensor(
    task_id = 'check_for_new_dump',
    http_conn_id = 'http_default',
    method = 'HEAD',
    poke_interval = 5,
    timeout = 15*60,
    endpoint = "{{ ti.xcom_pull(task_ids='print_date' )}}.export.CSV.zip",
    dag = dag
)

t2 = BashOperator(
    task_id='producer',
    bash_command="python /usr/local/kafka/airflow_producer_events.py {{ ti.xcom_pull(task_ids='print_date' )}}",
    retries=3,
    dag=dag)

t3 = BashOperator(
    task_id='consumer',
    bash_command='python /usr/local/kafka/airflow_consumer_events.py',
    retries=3,
    dag=dag)

sensor.set_upstream(t1);
t2.set_upstream(sensor);
t3.set_upstream(sensor);

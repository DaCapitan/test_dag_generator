from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
dag = DAG(
    dag_id='dag_123',
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False)

one = BashOperator(task_id='test_1', bash_command='echo "$TEST_VAR"', dag=dag)
two = BashOperator(task_id='test_2', bash_command='echo "$TEST_VAR_2"', dag=dag)
three = BashOperator(task_id='test_3', bash_command='echo "$PWD" && ls', dag=dag)
one >> two >> three

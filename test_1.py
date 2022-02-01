from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(dag_id='dag_to_test', schedule_interval='0 0 * * *', start_date=datetime(2022, 1, 25), catchup=False) as dag:
    one = BashOperator(task_id='task_1', bash_command='echo $TEST_VAR')
    two = BashOperator(task_id='task_2', bash_command='echo $TEST_VAR_2')
    three = BashOperator(task_id='task_3', bash_command='echo $PWD && ls -la')
    one >> two >> three

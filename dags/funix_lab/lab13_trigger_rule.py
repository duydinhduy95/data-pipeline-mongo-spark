from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime

import pendulum

def _branch():
    tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    day = tabDays[datetime.now().weekday()]
    return f"task_for_{day}"

with DAG('lab13_trigger_rule', start_date=datetime(2024, 1,1), schedule_interval='@daily', catchup=False) as dag:

    branching = BranchPythonOperator(task_id='branching', python_callable=_branch)

    task_for_monday = EmptyOperator(task_id='task_for_monday')
    task_for_tuesday = EmptyOperator(task_id='task_for_tuesday')
    task_for_wednesday = EmptyOperator(task_id='task_for_wednesday')
    task_for_thursday = EmptyOperator(task_id='task_for_thursday')
    task_for_friday = EmptyOperator(task_id='task_for_friday')
    task_for_saturday = EmptyOperator(task_id='task_for_saturday')

    branching >> [task_for_monday, task_for_tuesday, task_for_wednesday, task_for_thursday, task_for_friday, task_for_saturday]


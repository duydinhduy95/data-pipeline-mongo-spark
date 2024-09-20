from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

def import_tasks():
    with TaskGroup('imports', tooltip='Import tasks') as group:

        import_question = BashOperator(
            task_id='import_question',
            bash_command="mongoimport --uri='mongodb://root:root@mongodb:27017/asm2_db?authSource=admin' --type csv --headerline --file='/opt/airflow/data/Questions.csv'"
        )

        import_answer = BashOperator(
            task_id='import_answer',
            bash_command="mongoimport --uri='mongodb://root:root@mongodb:27017/asm2_db?authSource=admin' --type csv --headerline --file='/opt/airflow/data/Answers.csv'"
        )

    return group
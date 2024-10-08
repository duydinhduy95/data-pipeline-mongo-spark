from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset('/tmp/my_file.txt')

with DAG(
    dag_id='producer',
    schedule='@daily',
    catchup=False,
    start_date=datetime(2024,1,1)
) as dag:

    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, 'a+') as f:
            f.write('producer update')

    update_dataset()
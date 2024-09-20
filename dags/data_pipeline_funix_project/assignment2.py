from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
import os
from airflow.operators.bash import BashOperator
from data_pipeline_funix_project.download_files import download_tasks
from data_pipeline_funix_project.google_drive_downloader import GoogleDriveDownloader as gdd
from data_pipeline_funix_project.import_files import import_tasks
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import glob

from airflow.models.baseoperator import chain

def download_dataset(file_id, data_path, file_name):
    gdd.download_file_from_google_drive(file_id=f'{file_id}',
                                        dest_path=f'{data_path}{file_name}')

def _branching():
    check_Answers_existing = os.path.isfile('/opt/airflow/data/Answers.csv')
    check_Questions_existing = os.path.isfile('/opt/airflow/data/Questions.csv')

    if (check_Answers_existing and check_Questions_existing):
        return 'end'
    else:
        return 'clear_file'

with DAG(
    dag_id='assignment2',
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False
):

    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=_branching
    )

    clear_file = BashOperator(
        task_id='clear_file',
        bash_command='rm -f /opt/airflow/data/Answers.csv /opt/airflow/data/Questions.csv'
    )

    download_question = PythonOperator(
        task_id='download_question',
        python_callable=download_dataset,
        op_kwargs={
            'file_id': '1x3xABS2yT6Uo5AeZ2SVWMTyc-WDtqFUu',
            'data_path': '/opt/airflow/data/',
            'file_name': 'Questions.csv'
        }
    )

    download_answer = PythonOperator(
        task_id='download_answer',
        python_callable=download_dataset,
        op_kwargs={
            'file_id': '1oJUsQsa38Xp-B7KSD2YxRleg2B9Cyl-s',
            'data_path': '/opt/airflow/data/',
            'file_name': 'Answers.csv'
        }
    )

    import_question = BashOperator(
        task_id='import_question',
        bash_command="mongoimport --uri='mongodb://root:root@mongodb:27017/asm2_db?authSource=admin' --parseGrace skipRow --type csv --drop --headerline --ignoreBlanks --file='/opt/airflow/data/Questions.csv'"
    )

    import_answer = BashOperator(
        task_id='import_answer',
        bash_command="mongoimport --uri='mongodb://root:root@mongodb:27017/asm2_db?authSource=admin' --parseGrace skipRow --type csv --drop --headerline --ignoreBlanks --file='/opt/airflow/data/Answers.csv'"
    )

    spark_process = SparkSubmitOperator(
        task_id='spark_process',
        conn_id='spark_default',
        application='/opt/airflow/dags/data_pipeline_funix_project/spark_job.py',
        packages='org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
    )

    import_output_mongo = BashOperator(
        task_id='import_output_mongo',
        bash_command=f"mongoimport --uri='mongodb://root:root@mongodb:27017/asm2_db?authSource=admin' --parseGrace skipRow --collection=Output --type csv --headerline --file='/opt/airflow/data/output.csv'"
    )

    start >> branching >> end
    start >> branching >> clear_file >> download_question >> import_question >> spark_process >> import_output_mongo >> end
    start >> branching >> clear_file >> download_answer >> import_answer >> spark_process >> import_output_mongo >> end


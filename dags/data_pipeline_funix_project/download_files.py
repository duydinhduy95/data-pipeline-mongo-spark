from airflow.utils.task_group import TaskGroup
from data_pipeline_funix_project.google_drive_downloader import GoogleDriveDownloader as gdd
from airflow.operators.python import PythonOperator

def download_dataset(file_id, data_path, file_name):
    gdd.download_file_from_google_drive(file_id=f'{file_id}',
                                        dest_path=f'{data_path}{file_name}')

def download_tasks():
    with TaskGroup('downloads', tooltip='Download tasks') as group:

        download_question = PythonOperator(
            task_id='download_question',
            python_callable=download_dataset,
            op_kwargs={
                'file_id': '1pzhWKoKV3nHqmC7FLcr5SzF7qIChsy9B',
                'data_path': '/opt/airflow/data/',
                'file_name': 'Questions.csv'
            }
        )

        download_answer = PythonOperator(
            task_id='download_answer',
            python_callable=download_dataset,
            op_kwargs={
                'file_id': '1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y',
                'data_path': '/opt/airflow/data/',
                'file_name': 'Answers.csv'
            }
        )

    return group
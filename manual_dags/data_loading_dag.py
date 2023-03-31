"""
[OT254-98-99] H group, Sprint 2
Configures a Python Operator to execute the function that loads the data
from Universidad del Cine and Universidad de Buenos Aires to S3 bucket
"""

from datetime import datetime, timedelta
import logging
import os
import boto3
from botocore.exceptions import ClientError, DataNotFoundError
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from settings import *
from pathlib import Path


path = Path(__file__).resolve().parent.parent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s'
                    )


def upload_file_to_s3(file_name, object_name=None):
    """
    Takes the .txt files and loads them to S3 bucket

    Parameters
    ----------
    file_name : str
        Path to .txt file for Universidad del Cine/ Universidad de Buenos Aires

    object_name : None
        S3 object name

    Returns
    -------
    boolean
        returns True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(service_name='s3',
                             region_name=BUCKET_REGION,
                             aws_access_key_id=KEY_ID,
                             aws_secret_access_key=ACCESS_KEY
                             )

    try:
        # Uploads file if it doesn't exist or replace the old one
        s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    except DataNotFoundError as e:
        logging.error(e)
        return False
    return True


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

with DAG(
    dag_id='data_loading',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
) as dag:

    # Uploads data from Universidad del Cine
    cine_data_loading_task = PythonOperator(
        task_id='cine_data_loading',
        python_callable=upload_file_to_s3,
        op_kwargs={
            'file_name': str(path) + '/files/cine_univ.txt'
        }
    )

    # Uploads data from Universidad de Buenos Aires
    ba_data_loading_task = PythonOperator(
        task_id='ba_data_loading',
        python_callable=upload_file_to_s3,
        op_kwargs={
            'file_name': str(path) + '/files/ba_univ.txt'
        }
    )

[cine_data_loading_task, ba_data_loading_task]

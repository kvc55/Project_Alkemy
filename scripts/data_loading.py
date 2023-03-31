import logging
import os
import boto3
from botocore.exceptions import ClientError, DataNotFoundError
from settings import *
from pathlib import Path


path = Path(__file__).resolve().parent.parent


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

"""
[OT254-67] H group, Sprint 1
Configures a Python Operator to extract data from DB using .sql files
and save the information into .csv files
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import exc
import logging
from settings import *


path = Path(__file__).resolve().parent.parent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s'
                    )


def data_extraction(cine_path, ba_path):
    """
    Extracts data from two tables in postgres DB using .sql
    files and save it into .csv files

    Parameters
    ----------
    cine_path : str
        Path to .sql file for Universidad del Cine

    ba_path : str
        Path to .sql file for Universidad del Buenos Aires
    """

    global path

    # Tries to connect to DB, if cannot, shows error
    try:
        logging.info('Connecting to database...')
        engine = create_engine(DB_CONNECTION)
        conn = engine.connect()
    except exc.OperationalError as err:
        logging.error('Something went wrong...')
        logging.error(err)
    else:
        logging.info('Connection done successfully')

    # Start data extraction
    univ_dict = {
                  'cine_univ': cine_path,
                  'ba_univ': ba_path
                }

    # Create directory to save .csv files if it doesn't exist
    b_path = str(path) + '/files/'
    os.makedirs(b_path, mode=0o777, exist_ok=True)

    for univ, path in univ_dict.items():
        # Generate .csv paths
        f_path = univ + '.csv'
        final_path = b_path + f_path

        # Read .sql files and convert into dataframes
        try:
            query = open(path, 'r')
        except FileNotFoundError as e:
            logging.error('Something went wrong..')
            logging.error(e)
        else:
            df = pd.read_sql(query.read(), conn)

            # Save .csv files
            df.to_csv(final_path, index=False, encoding='utf-8')


DEFAULT_ARGS = {
    'owner': 'airflow',    
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

with DAG(
    dag_id='data_extraction',
    description="""
                Extracts data from 2 tables
                and save it into .csv files
                """,
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 18)
) as dag: 
    data_extraction_task = PythonOperator(
        task_id='data_extraction',
        python_callable=data_extraction,
        op_args=[str(path) + '/dags/sql/cine_univ.sql',
                 str(path) + '/dags/sql/ba_univ.sql']
    )

data_extraction_task

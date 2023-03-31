"""
[OT254-43] H group, Sprint 1
Configures retries with connection to DB.
Configures retry for DAG's tasks for: Universidad del Cine
and Universidad de Buenos Aires.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from sqlalchemy import create_engine
from sqlalchemy import exc
import logging
from settings import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s'
                    )


def check_db_connection():

    """
    Checks connection to DB, if connection fails, shows error

    """

    try:
        logging.info('Connecting to database...')
        engine = create_engine(DB_CONNECTION)
        conn = engine.connect()
    except exc.OperationalError as err:
        if "Connection refused" in str(err):
            connected = False
            logging.error('Something went wrong...')
            logging.error(err)
        else:
            raise
    else:
        connected = True
        logging.info('Connection done successfully')
    finally:
        if connected:
            conn.close()


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

with DAG(
        dag_id='check_db_connection',
        description='Checks connection to database',
        default_args=DEFAULT_ARGS,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 18)
) as dag:
    chk_db_conn_task = PythonOperator(
        task_id='check_db_connection',
        python_callable=check_db_connection
    )

chk_db_conn_task

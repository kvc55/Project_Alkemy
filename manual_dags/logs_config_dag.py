"""
[OT254-51] H group, Sprint 1
Configures the logs for Universidad del Cine and Universidad de Buenos Aires.
Makes a log when a dag is initialized with the logger name.
Log format: %Y-%m-%d - logger_name - message.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
import logging


# Logs configuration format: %Y-%m-%d - logger_name - message
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%Y-%m-%d'
                    )


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}


with DAG(
    dag_id='logs_config_dag',
    description="""
                Configures the logs for group H universities
                """,
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 18)
) as dag:
    logs_config_task = DummyOperator(
        task_id='logs_config'
    )

logs_config_task

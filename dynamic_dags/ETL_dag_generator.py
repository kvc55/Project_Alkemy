"""
[OT254-115-123] H group, Sprint 2
Configures the DAG for the ETL process for Universidad del Cine and
Universidad de Buenos Aires using dag-factory library.
Configures logs to show them in console and file with
%(asctime)s_%(name)s_%(levelname)s%(message)s format.
"""

import dagfactory
import logging.config
from os import path
from airflow import DAG


# Loads logs configuration using .cfg file
logs_file_path = path.join(path.dirname(path.abspath(__file__)), 'logs_config.cfg')
logging.config.fileConfig(logs_file_path)
logging.getLogger('root')


# Generates dag for ETL process automatically using .yaml file
dag_path = path.join(path.dirname(path.abspath(__file__)), 'ETL_dag.yaml')
dag_factory = dagfactory.DagFactory(dag_path)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())

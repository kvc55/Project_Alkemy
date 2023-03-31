"""
[OT254-75-83] H group, Sprint 1
Configures a Python Operator to execute the function that processes the data
from Universidad del Cine and Universidad de Buenos Aires.
Python function that processes the .csv files from Universidad del Cine
and Universidad de Buenos Aires to normalize data and saves results into .txt files.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from pathlib import Path
import pandas as pd
import os

path = Path(__file__).resolve().parent.parent


def data_processing(cine_p_csv, ba_p_csv, url):

    """
    Takes the .csv files and processes the information. Results are saved into .txt files

    Parameters
    ----------
    cine_p_csv : str
        Path to .csv file for Universidad del Cine

    ba_p_csv : str
        Path to .csv file for Universidad de Bueno Aires

    url : str
        URL to .csv file for postal codes and locations
    """

    # url to download postal codes and locations .csv file
    pc_loc_url = url
    pc_loc_df = pd.read_csv(pc_loc_url)

    # Covert data from postal codes and locations dataframe into lower case
    pc_loc_df = pc_loc_df.apply(lambda x: x.astype(str).str.lower())

    # Renames columns
    pc_loc_df.rename(columns={'codigo_postal': 'postal_code',
                              'localidad': 'location'}, inplace=True)

    # Processing data from Universidad del Cine and Universidad de Buenos Aires .csv files
    csv_paths = {
                  'cine_univ': cine_p_csv,
                  'ba_univ': ba_p_csv
                }

    for univ, csv_path in csv_paths.items():
        df = pd.read_csv(csv_path)

        # Convert data into lower case
        df = df.apply(lambda x: x.astype(str).str.lower())

        # Replace values
        old_values = ['-', 'f', 'm', 'days']
        new_values = [' ', 'female', 'male', '']
        df.replace(to_replace=old_values, value=new_values, inplace=True, regex=True)

        # Convert age column represented in days to years
        df.age = ((df.age.astype(int)) / 365).astype(int)

        # Change inscription date format
        df.inscription_date = list(map(lambda x: datetime.strptime(x, '%d %m %Y').strftime('%Y%m%d'),
                                       df.inscription_date))

        # Combine dataframes
        if univ == 'cine_univ':
            df = pd.merge(df, pc_loc_df, on=['location'], how='inner')
        else:
            df = pd.merge(df, pc_loc_df, on=['postal_code'], how='inner')

        # Save dataframes into .txt files
        b_path = str(path) + '/files/'
        os.makedirs(b_path, mode=0o777, exist_ok=True)
        txt_path = b_path + univ + '.txt'
        df.to_string(txt_path, index=False)


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

with DAG(
        dag_id='data_processing',
        description="""
                Executes the function that manages 
                data processing
                """,
        default_args=DEFAULT_ARGS,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 18)
) as dag:
    data_processing_task = PythonOperator(
        task_id='data_processing',
        python_callable=data_processing,
        op_args=[str(path) + '/files/cine_univ.csv',
                 str(path) + '/files/ba_univ.csv',
                 'https://drive.google.com/u/0/uc?id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ&export=download'
                 ]
    )

data_processing_task

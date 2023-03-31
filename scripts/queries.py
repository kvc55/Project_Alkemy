"""
[OT254-19] H group, Sprint 1
Makes queries to extract data between 09/01/2020 to 02/01/2021 from Universidad
del Cine and Universidad de Buenos Aires tables. If the task fails, shows error after 5 retries.
The queries are done from .sql files.
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import exc
from pathlib import Path
import logging
from settings import *


logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def data_extraction(conn):

    """
    Extracts data from two tables in postgres DB using .sql files and combine it with
    an external .csv file. Final data is saved into .csv files

    Parameters
    ----------
    conn : Connection
        Connection to DB
    """

    path = Path(__file__).resolve().parent.parent

    # Start extracting data
    csv_paths = {}

    # Paths to .sql files
    univ_dict = {
                  'cine_univ': str(path) + '/airflow/dags/sql/cine_univ.sql',
                  'ba_univ': str(path) + '/airflow/dags/sql/ba_univ.sql'
                }

    # Create directory to save .csv files if it doesn't exist
    b_path = path / 'files/'
    b_path.mkdir(parents=True, exist_ok=True)

    for univ, path in univ_dict.items():
        # Generate .csv paths
        f_path = univ + '.csv'
        final_path = b_path / f_path

        # Read .sql files and convert into dataframes
        try:
            query = open(path, 'r')
        except FileNotFoundError as e:
            logging.error('Something went wrong...')
            logging.error(e)
        else:
            df = pd.read_sql(query.read(), conn)

            # Save .csv files
            df.to_csv(final_path, index=False, encoding='utf-8')
            csv_paths[univ] = final_path

    # url to download postal codes and locations .csv file
    pc_loc_url = 'https://drive.google.com/u/0/uc?id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ&export=download'
    pc_loc_df = pd.read_csv(pc_loc_url)

    # Renames columns
    pc_loc_df.rename(columns={'codigo_postal': 'postal_code',
                              'localidad': 'location'}, inplace=True)

    # Combine dataframes
    for univ, path in csv_paths.items():
        df = pd.read_csv(path)

        if univ == 'cine_univ':
            df = pd.merge(df, pc_loc_df, on=['location'], how='inner')
        else:
            df = pd.merge(df, pc_loc_df, on=['postal_code'], how='inner')

        # Save dataframes into .csv files
        final_path = b_path / (univ + '_new.csv')
        df.to_csv(final_path, index=False)

    return


if __name__ == '__main__':
    # Tries to connect to DB, if connection fails, shows error after 5 retries
    for retry in range(5):
        if retry < 5:
            try:
                logging.info('Connecting to database...')
                engine = create_engine(DB_CONNECTION)
                conn = engine.connect()
            except exc.OperationalError as err:
                if retry == 4:
                    logging.error('Something went wrong...')
                    logging.error(err)
                    break
            else:
                logging.info('Connection done successfully')
                data_extraction(conn)
                break

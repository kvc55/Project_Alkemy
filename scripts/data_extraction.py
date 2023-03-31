from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import exc
import logging
from settings import *


path = Path(__file__).resolve().parent.parent


def data_extraction(cine_path, ba_path):
    """
    Extracts data from two tables in postgres DB using .sql
    files and saves it into .csv files

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

        # Read .sql files and convert them into dataframes
        try:
            query = open(path, 'r')
        except FileNotFoundError as e:
            logging.error('Something went wrong..')
            logging.error(e)
        else:
            df = pd.read_sql(query.read(), conn)

            # Save .csv files
            df.to_csv(final_path, index=False, encoding='utf-8')

    return

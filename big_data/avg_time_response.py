"""
[OT254-139-147] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the average time a post was active
considering positions 300-400.
"""

import xml.etree.ElementTree as ET
import logging.config
import operator
from functools import reduce
from os import path
from datetime import datetime


# Loads logs configuration using .cfg file
logs_file_path = path.join(path.dirname(path.abspath(__file__)), 'logs_config.cfg')
logging.config.fileConfig(logs_file_path)
logger = logging.getLogger('logs_big_data')


def get_data(file):

    """
    Takes a .xml file and parses it

    Parameters
    ----------
    file : str
        Name of the .xml file with posts data from StackOverflow

    Returns
    -------
    xml.etree.ElementTree.Element
        Returns parsed file
    """

    tree = ET.parse(file)
    root = tree.getroot()

    return root


def chunkify(data, chunk_len):

    """
    Takes the parsed .xml file and splits it in n equal chunks

    Parameters
    ----------
    data : xml.etree.ElementTree.Element
        Parsed .xml file

    chunk_len : int
        Chunk's length

    Returns
    -------
    list
        returns list of chunks
    """

    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

    return chunks


def post_time_active(row):

    """
    Takes a chunk's row and gets the value of "Id", "CreationDate" and "LastActivityDate" attributes.
    Calculates the time period a post was opened

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    dict
        returns dict with "Id" as key and ("LastActivityDate" - "CreationDate") as value
    """

    post_id = row.get("Id")
    creation_date = datetime.strptime(row.get("CreationDate"), "%Y-%m-%dT%H:%M:%S.%f")
    last_activity_date = row.get("LastActivityDate")

    if last_activity_date is not None:
        last_activity_date = datetime.strptime(last_activity_date, "%Y-%m-%dT%H:%M:%S.%f")
        time_active = last_activity_date - creation_date
    else:
        return

    return {post_id: time_active}



def reducer(dict1, dict2):

    """
    Updates first dictionary with values of the second dictionary

    Parameters
    ----------
    dict1 : dict
        Period of time a post was active

    dict2 : dict
        Period of time a post was active

    Returns
    -------
    dict
        returns updated dictionary
    """

    for key, value in dict2.items():
        if key in dict1.keys():
            dict1[key] += value
        else:
            dict1[key] = value

    return dict1


def mapper(chunk):

    """
    Takes a chunk of data and returns a dictionary with the period of time the post was active

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    dict
        returns the period of time the post was active
    """

    time_active = list(map(post_time_active, chunk))

    dict_chunk = reduce(reducer, time_active)

    return dict_chunk


def avg_time(list_values):

    """
    Takes a list of time periods a post was active and calculates the average time
    for interval 300 to 400

    Parameters
    ----------
    list_values : list
        Time periods

    Returns
    -------
    datetime.timedelta
        returns the average time a post was active
    """

    list_values_300_400 = list_values[300:400]
    avg_time = reduce(operator.add, list_values_300_400) / len(list_values_300_400)

    return avg_time


if __name__ == '__main__':

    try:
        # Parses the .xml file
        logger.info('Starting to parse .xml file...')
        data = get_data('posts.xml')
    except FileNotFoundError as e:
        logger.error('Something went wrong..')
        logger.error(e)
    else:
        # Data splitter
        logger.info('Starting to split data...')
        data_chunks = chunkify(data, 50)

        # Main mapping function
        logger.info('Starting to map data...')
        mapped = list(map(mapper, data_chunks))

        # Main reduction function
        logger.info('Starting to reduce data...')
        reduced = reduce(reducer, mapped)

        # Average time a post was active
        avg_time = avg_time(list(reduced.values()))
        logger.info(f"Average time a post was active: {avg_time}")



"""
[OT254-139-147] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
"""

import xml.etree.ElementTree as ET
import logging.config
from functools import reduce
from collections import Counter
from os import path


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


def get_view_count(row):

    """
    Takes a chunk's row and gets the value of "ViewCount" attribute

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    str
        returns number of views from a post
    """

    posts_views = row.get("ViewCount")

    return posts_views


def mapper(chunk):

    """
    Takes a chunk of data and counts occurrences from attribute "ViewCount"

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns occurrences count for attribute "ViewCount"
    """

    posts_views = list(map(get_view_count, chunk))

    return Counter(posts_views)


def reducer(count1, count2):

    """
    Takes occurrences count for attribute "ViewCount" from chunks and updates count from first chunk

    Parameters
    ----------
    count1 : collections.Counter
        Counter to be update

    count2 : collections.Counter
        Counter used to do updates

    Returns
    -------
    collections.Counter
        returns updated counter
    """

    count1.update(count2)

    return count1


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

        # Top 10 least viewed posts
        top10_least_viewed = reduced.most_common(10)
        logger.info(f"Top 10 posts least viewed is: {top10_least_viewed}")


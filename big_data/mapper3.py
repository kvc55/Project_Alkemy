#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the average time a post was active considering positions 300-400.
Modified script from 'avg_time_response.py' to use with Hadoop.
"""

import xml.etree.ElementTree as ET
import sys


def get_data(file):

    """
    Takes a .xml file and parses it

    Parameters
    ----------
    file : TextIO
        .xml file with posts data from StackOverflow

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
    Takes a chunk's row and gets the value of "CreationDate" and "LastActivityDate" attributes.

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns list with "CreationDate" and "LastActivityDate"
    """

    creation_date = row.get("CreationDate")
    last_activity_date = row.get("LastActivityDate")

    if last_activity_date is not None:
        return [creation_date, last_activity_date]
    else:
        return


def mapper(chunk):

    """
    Takes a chunk of data and writes pairs of creation date and last activity date to STDOUT

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file
    """

    time_active = list(map(post_time_active, chunk))

    for item in time_active:
        print('%s%s%s' % (item[0], '\t', item[1]))


if __name__ == '__main__':

    # Parses the .xml file
    data = get_data(sys.stdin)

    # Data splitter
    data_chunks = chunkify(data, 50)

    # Main mapping function
    mapped = list(map(mapper, data_chunks))

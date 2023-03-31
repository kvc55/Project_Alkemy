#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
Modified script from 'top10_least_viewed.py' to use with Hadoop.
"""

import sys
import xml.etree.ElementTree as ET
from collections import Counter


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
    """

    posts_views = list(map(get_view_count, chunk))
    posts_views = Counter(posts_views)

    for view, count in posts_views.items():
        print('%s%s%d' % (view, '\t', count))


if __name__ == '__main__':

    # Parses the .xml file
    data = get_data(sys.stdin)

    # Data splitter
    data_chunks = chunkify(data, 50)

    # Main mapping function
    mapped = list(map(mapper, data_chunks))






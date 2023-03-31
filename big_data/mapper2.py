#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the most common words per type of post.
Modified script from 'top10_most_common_words.py' to use with Hadoop.
"""

import xml.etree.ElementTree as ET
import re
import sys
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
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


def get_body(row):

    """
    Takes a chunk's row and gets the value of "PostTypeId" and "Body" attributes.
    Processes text within post's body.

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns list with "PostTypeId" and "Body" (words)
    """

    post_type = row.get("PostTypeId")
    body_posts = row.get("Body")

    # Converts to lowercase
    body_posts = body_posts.lower()

    # Removes punctuation and non_alphanumeric characters
    body_posts = re.sub(r'[^\w\s]', '', body_posts)

    # Removes numbers
    body_posts = re.sub(r'[0-9]+', '', body_posts)

    # Removes any single letter
    body_posts = re.sub('(\\b[A-Za-z] \\b|\\b [A-Za-z]\\b)', '', body_posts)

    # Removes stopwords
    stop_words = set(stopwords.words('english'))
    body_posts = word_tokenize(body_posts)
    body_posts = [w for w in body_posts if w not in stop_words]

    # Lemmatization
    wordnet_lemmatizer = WordNetLemmatizer()
    body_posts = [wordnet_lemmatizer.lemmatize(w) for w in body_posts]

    body_posts = [f'{post_type}-{word}' for word in body_posts]

    return body_posts


def mapper(chunk):

    """
    Takes a chunk of data and counts word occurrences per type of post

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file
    """

    body_posts = list(map(get_body, chunk))
    body_posts = [word for sublist in body_posts for word in sublist]

    body_posts = Counter(body_posts)

    for word, count in body_posts.items():
        print('%s%s%d' % (word, '\t', count))


if __name__ == '__main__':

    # Parses the .xml file
    data = get_data(sys.stdin)

    # Data splitter
    data_chunks = chunkify(data, 50)

    # Main mapping function
    mapped = list(map(mapper, data_chunks))

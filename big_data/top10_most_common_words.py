"""
[OT254-139-147] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the most common words per type of post.
"""

import xml.etree.ElementTree as ET
import logging.config
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
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
    dict
        returns dict with "PostTypeId" and processed "Body" as key and value
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

    return {post_type: body_posts}


def reducer(dict1, dict2):

    """
    Updates first dictionary with values of the second dictionary

    Parameters
    ----------
    dict1 : dict
        Dictionary of words

    dict2 : dict
        Dictionary of words

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
    Takes a chunk of data and returns a dictionary with words per type of post

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    dict
        returns dictionary with words per type of post
    """

    body_posts = list(map(get_body, chunk))

    dict_chunk = reduce(reducer, body_posts)

    return dict_chunk


def words_counter(dict_question_answer):

    """
    Takes a dictionary of words as values per type of post as key and counts words' occurrences

    Parameters
    ----------
    dict_question_answer : dict
        Takes the type of post as key and a list of words as values

    Returns
    -------
    collections.Counter
        returns the occurrences of words in post type: question

    collections.Counter
        returns the occurrences of words in post type: answer
    """

    for key, value in dict_question_answer.items():
        if key == '1':
            words_question = Counter(reduced[key])
        else:
            words_answer = Counter(reduced[key])

    return words_question, words_answer


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

        # Top 10 most common words in posts per type of post
        words_question, words_answer = words_counter(reduced)
        logger.info(f"Top 10 most common words in questions: {words_question.most_common(10)}")
        logger.info(f"Top 10 most common words in answers: {words_answer.most_common(10)}")


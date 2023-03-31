"""
[OT254-179-187] H group, Sprint 4
Unit test for the file that processes and analyses StackOverflow data
to obtain the top 10 of the most common words per type of post.
"""

import unittest
from big_data.top10_most_common_words import *


class TestMostCommonWords(unittest.TestCase):

    """
    Tests basic functionality of mapreduce file that obtains the top 10 of most common words

    Attribute
    ----------
    file: str
        Path to the file used as data.

    data: xml.etree.ElementTree.Element
        Parsed file
    """

    file = path.join(path.dirname(path.abspath(__file__)), '../big_data/posts.xml')
    data = ET.parse(file).getroot()

    # Disables logging under critical level
    def setUp(self):
        logging.disable(logging.CRITICAL)

    # Enables logging after executing tests
    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_file_parsing(self):

        """
        Tests basic functionality when parsing .xml file
        - Checks if function raises exception when it doesn't find path
        - Checks if returned value has the correct type
        """

        file1 = 'data.xml'
        msg = 'Output value must be type xml.etree.ElementTree.Element'

        with self.assertRaises(FileNotFoundError):
            get_data(file1)
        self.assertIsInstance(get_data(file), ET.Element, msg)

    def test_mapper(self):

        """
        Tests basic functionality of mapper function:
        - Checks that the value returned is not None
        - Checks the correct type of the returned value
        """

        msg = 'Output value must not be None'
        msg1 = 'Output value must be type dict'

        self.assertIsNotNone(mapper(self.data), msg)
        self.assertIs(type(mapper(self.data)), dict, msg1)

    def test_reducer(self):

        """
        Tests basic functionality of reducer function:
        - Checks that the value returned is not None
        - Checks the correct type of the returned value
        """

        msg = 'Output value must not be None'
        msg1 = 'Output value must be type dict'

        # Input data to check function
        dict1 = {'0': ['dog', 'table', 'bird'], '1': ['table', 'perl']}
        dict2 = {'1': ['chair', 'dog'], '0': ['python', 'sql', 'mouse']}

        self.assertIsNotNone(reducer(dict1, dict2), msg)
        self.assertIs(type(reducer(dict1, dict2)), dict, msg1)

    def test_top10_most_common_words(self):

        """
        Tests if results of mapreduce are correct:
        - Checks if returned value has the correct type
        - Checks if results' length are fine
        - Checks if returned values are correct
        """

        msg = 'Output value must be type tuple'

        # Lists of expected returned values
        expected1 = [('question', 22997), ('answer', 12129), ('pi', 8929), ('blockquote', 7071), ('would', 7057),
                     ('user', 6979), ('tag', 5885), ('like', 5537), ('site', 5330), ('one', 5030)]
        expected2 = [('question', 137), ('tag', 87), ('post', 76), ('vote', 72), ('ul', 64),
                     ('answer', 63), ('community', 53), ('hwhat', 47), ('new', 39), ('site', 37)]

        self.assertIs(type(top10_most_common_words()), tuple, msg)
        self.assertEqual(len(top10_most_common_words()), 2)
        self.assertEqual(top10_most_common_words(), (expected1, expected2))


if __name__ == '__main__':
    unittest.main()

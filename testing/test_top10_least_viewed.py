"""
[OT254-179-187] H group, Sprint 4
Unit test for the file tha processes and analyses StackOverflow
data to obtain the top 10 of the least viewed posts.
"""

import collections
import unittest
from big_data.top10_least_viewed import *


class TestLeastViewed(unittest.TestCase):

    """
    Tests basic functionality of mapreduce file that obtains the top 10 of the least viewed posts

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
        msg1 = 'Output value must be type Counter'

        self.assertIsNotNone(mapper(self.data), msg)
        self.assertIs(type(mapper(self.data)), collections.Counter, msg1)

    def test_reducer(self):

        """
        Tests basic functionality of reducer function:
        - Checks that the value returned is not None
        - Checks the correct type of the returned value
        """

        msg = 'Output value must not be None'
        msg1 = 'Output value must be type Counter'

        # Input data to check function
        count1 = Counter([0, 1, 3, 5, 3])
        count2 = Counter([0, 6, 4, 3, 5])

        self.assertIsNotNone(reducer(count1, count2), msg)
        self.assertIs(type(reducer(count1, count2)), collections.Counter, msg1)

    def test_top10_least_viewed(self):

        """
        Tests if results of mapreduce are correct:
        - Checks if returned value has the correct type
        - Checks if results' length are fine
        - Checks if returned values are correct
        """

        msg = 'Output value must be type list'

        # List of expected returned values
        expected = [('0', 41301), ('40', 163), ('44', 160), ('54', 160), ('58', 159),
                    ('67', 147), ('52', 146), ('63', 146), ('38', 146), ('62', 145)]

        self.assertIs(type(top10_least_viewed()), list, msg)
        self.assertEqual(len(top10_least_viewed()), 10)
        self.assertEqual(top10_least_viewed(), expected)


if __name__ == '__main__':
    unittest.main()

"""
[OT254-179-187] H group, Sprint 4
Unit test for the file that processes and analyses StackOverflow data to
obtain the average time a post was active considering positions 300-400.
"""
from datetime import timedelta
import unittest
from big_data.avg_time_response import *


class TestAvgResponseTime(unittest.TestCase):

    """
    Tests basic functionality of mapreduce file that obtains the average response time

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
        Tests basic functionality of mapper function:
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
        - Checks if returned value is correct
        """

        msg = 'Output value must be type datetime.timedelta'

        # Expected returned value
        expected = '50 days, 3:46:31.406190'

        self.assertIsInstance(avg_time_response(), timedelta, msg)
        self.assertEqual(str(avg_time_response()), expected)


if __name__ == '__main__':
    unittest.main()

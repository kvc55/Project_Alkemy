#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the average time a post was active considering positions 300-400.
Modified script from 'avg_time_response.py' to use with Hadoop.
"""

import sys
import operator
from functools import reduce
from datetime import datetime


def reducer():

    """
    Takes input from STDIN and filters data per time period a post was active
    at positions 300-400 to calculate the average time
    """

    time_active_list = []

    # Parses the input line by line
    for line in sys.stdin:
        # Removes leading and trailing whitespace
        line = line.strip()

        # Splits the line in date a post was created and the last activity
        creation_date, last_activity_date = line.split('\t', 1)

        try:
            creation_date = datetime.strptime(creation_date, "%Y-%m-%dT%H:%M:%S.%f")
            last_activity_date = datetime.strptime(last_activity_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            continue

        # Calculates the period of time a post was active
        time_active = last_activity_date - creation_date

        # Saves all time period values
        time_active_list.append(time_active)

    # Selects only the values within positions 300-400 and calculates the average
    list_values_300_400 = time_active_list[300:400]
    avg_time = reduce(operator.add, list_values_300_400) / len(list_values_300_400)

    # Writes result on STDOUT
    print(avg_time)


if __name__ == '__main__':
    reducer()

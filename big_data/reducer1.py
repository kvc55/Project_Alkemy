#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
Modified script from 'top10_least_viewed.py' to use with Hadoop.
"""

import sys


def reducer():

    """
    Takes input from STDIN and count word occurrences
    """

    current_view = None
    current_count = 0
    view = None
    list_count_views = []

    # Parses the input line by line
    for line in sys.stdin:
        # Removes leading and trailing whitespace
        line = line.strip()

        # Splits the line in "ViewCount" attribute and its occurrence
        view, count = line.split('\t', 1)

        try:
            count = int(count)
        except ValueError:
            continue

        # Counts the "ViewCount" attribute occurrence
        if current_view == view:
            current_count += count
        else:
            if current_view:
                list_count_views.append([current_view, current_count])
            current_count = count
            current_view = view

    # Saves the last pair "ViewCount", count
    if current_view == view:
        list_count_views.append([current_view, current_count])

    # Writes result on STDOUT
    to10_least_viewed = sorted(list_count_views, key=lambda x: x[1], reverse=True)[:10]
    print(to10_least_viewed)


if __name__ == '__main__':
    reducer()

#!/usr/bin/python3

"""
[OT254-163] H group, Sprint 3
Processes and analyses StackOverflow data to obtain the top 10 of the most common words per type of post.
Modified script from 'top10_most_common_words.py' to use with Hadoop.
"""

import sys


def reducer():

    """
    Takes input from STDIN and count word occurrences
    """

    current_word = None
    current_count = 0
    word = None
    list_count_word = []

    # Parses the input line by line
    for line in sys.stdin:
        # Removes leading and trailing whitespace
        line = line.strip()

        # Splits the line in word and its occurrence
        word, count = line.split('\t', 1)

        try:
            count = int(count)
        except ValueError:
            continue

        # Counts the word occurrence
        if current_word == word:
            current_count += count
        else:
            if current_word:
                list_count_word.append([current_word, current_count])
            current_count = count
            current_word = word

    # Saves the last pair word, count
    if current_word == word:
        list_count_word.append([current_word, current_count])

    # Writes result on STDOUT
    to10_most_common_words = sorted(list_count_word, key=lambda x: x[1], reverse=True)[:10]
    print(to10_most_common_words)


if __name__ == '__main__':
    reducer()

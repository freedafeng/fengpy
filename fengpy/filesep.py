#!/usr/bin/python

"""
This will be used as a command line tool to only extract specific lines in a file
"""
import sys


def get_lines(input_file, start, end):
    with open(input_file) as fin:
        cnt = 0
        for line in fin:
            cnt += 1
            if start <= cnt <= end:
                print line,
            elif cnt > end:
                return


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print 'Usage: filesep input output start end'
    else:
        get_lines(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))


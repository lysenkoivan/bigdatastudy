#!/usr/bin/env python

import sys


def read_input(file):
    for line in file:
        line.strip()
        yield line.split()


def main():
    input_data = read_input(sys.stdin)
    for words in input_data:
        print '%.2f\t%s\t%s' % (float(words[6]), words[7], words[0])


if __name__ == "__main__":
    main()

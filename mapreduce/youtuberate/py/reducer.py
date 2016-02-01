from itertools import groupby
from operator import itemgetter
import sys

def get_rating(item):
    return item[0]

for line in sys.stdin:
    line = line.strip()
    data = line.split("\t")

    for rate, group in groupby(data, itemgetter(0)):  # emulates shuffle I suppose
        aaa = sorted(((rating, id) for rate, rating, id in group), key=get_rating)
        print "" #TODO push output to file with rate name
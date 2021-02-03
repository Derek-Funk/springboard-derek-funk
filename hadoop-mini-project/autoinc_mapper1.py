#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # [derive mapper output key values]
    cells = line.split(',')
    key = cells[2]
    value = '%s\t%s\t%s' % (cells[1], cells[3], cells[5])
    print '%s\t%s' % (key, value)

# testing
# $ cat data.csv | python autoinc_mapper1.py | sort

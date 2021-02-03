#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # [derive mapper output key values]
    cells = line.split('\t')
    make = cells[1]
    year = cells[2].strip()
    key = make + year
    value = 1
    print '%s\t%s' % (key, value)

# testing
# $ cat data.csv | python autoinc_mapper1.py | sort | python autoinc_reducer1.py | python autoinc_mapper2.py | sort

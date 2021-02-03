#!/usr/bin/env python

import sys

# [Define group level master information]
current_makeYear = None
current_count = 0

def reset():
    # [Logic to reset master info for every new group]
    global current_makeYear
    global current_count

    current_makeYear = None
    current_count = 0
    return

# Run for end of every group
def flush():
    # [Write the output]
    print '%s\t%s' % (current_makeYear, current_count)
    return

# input comes from STDIN
for line in sys.stdin:

    # [parse the input we got from mapper and update the master info]
    cells = line.split('\t')
    makeYear = cells[0]
    count = int(cells[1].strip())

    # [detect key changes]
    if current_makeYear != makeYear:
        if current_makeYear != None:
            # write the result to STDOUT
            flush()
        reset()

    # [update more master info after the key change handling]
    current_makeYear = makeYear
    current_count += count
# do not forget to output the last group if needed!
flush()

# testing
# $ cat data.csv | python3 autoinc_mapper1.py | sort | python3 autoinc_reducer1.py | python3 autoinc_mapper2.py | sort | python3 autoinc_reducer2.py
# file = open('data4.txt', 'r')
# lines = file.readlines()
# file.close()

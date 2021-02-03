#!/usr/bin/env python

import sys

# [Define group level master information]
current_vin = None
current_make = None
current_year = None
has_accident = False

def reset():
    # [Logic to reset master info for every new group]
    global current_vin
    global current_make
    global current_year
    global has_accident

    current_vin = None
    current_make = None
    current_year = None
    has_accident = False
    return

# Run for end of every group
def flush():
    # [Write the output]
    if has_accident:
        print '%s\t%s\t%s' % (current_vin, current_make, current_year)
    return

# input comes from STDIN
for line in sys.stdin:

    # [parse the input we got from mapper and update the master info]
    cells = line.split('\t')
    vin = cells[0]
    record_type = cells[1]
    make = cells[2]
    year = cells[3].strip()


    # [detect key changes]
    if current_vin != vin:
        if current_vin != None:
            # write the result to STDOUT
            flush()
        reset()

    # [update more master info after the key change handling]
    if make != '':
        current_make = make
        current_year = year
    if record_type == 'A':
        has_accident = True
    current_vin = vin
# do not forget to output the last group if needed!
flush()

# testing
# $ cat data.csv | python autoinc_mapper1.py | sort | python autoinc_reducer1.py

# settings
from pyspark import SparkContext
sc = SparkContext('local', 'Spark Mini-Project')

# read in file
raw_rdd = sc.textFile('data.csv')
# raw_rdd.foreach(print)

# extract_vin_key_value - keep certain columns (vin, type, make, year)
# raw_rdd.take(1)
# notice each RDD row is a list with one string for the entire row, thus needs to get split
# key = VIN, value = list of type, make, year
def extract_vin_key_value(row):
    values = row.split(',')
    return (values[2], (values[1], values[3], values[5]))
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
# vin_kv.foreach(print)

# populate_make - copy make, year to accident types, then keep just accident types
def populate_make(values):
    values_list = list(values)
    type_i = [x for x in values_list if x[0]=='I']
    type_a = [x for x in values_list if x[0]=='A']
    combined = list(zip(type_i, type_a))
    return [(x[0][1:]) for x in combined]
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
# enhance_make.foreach(print)

# extract_make_key_value - new key of make/year, give each count of 1
def extract_make_key_value(row):
    return (row[0] + '-' + row[1], 1)
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
# make_kv.foreach(print)

# sum accidents by make/year key
final_rdd = make_kv.reduceByKey(lambda x,y: x+y)
final_rdd.foreach(print)
# Summary of Files
* data.csv - input file of 16 car transactions of various types (I = initial sale, R = repair, A = accident)
* autoinc_spark.py - pyspark file to aggregate number of accidents per car make/year using RDDs
* execution_log.txt - example of output when running locally in terminal

# How to Reproduce
* ensure you have spark and pyspark installed
* create a directory with the data file and python script
* in a terminal, run "spark-submit autoinc_spark.py"
* verify terminal prints following output:
    * 1 accident for Nissan 2003
    * 1 accident for Mercedes 2015
    * 2 accidents for Mercedes 2016
* To debug the python script, open the python script and send specific statements to a pyspark shell. There are several print statements in the script that can be uncommented to see intermediate RDDs.
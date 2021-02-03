hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar \
  -input /hadoop-mini-project/input/data.csv \
  -mapper /hadoop-mini-project/autoinc_mapper1.py -file /hadoop-mini-project/autoinc_mapper1.py \
  -reducer /hadoop-mini-project/autoinc_reducer1.py -file /hadoop-mini-project/autoinc_reducer1.py \
  -output /hadoop-mini-project/output/all_accidents

hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar \
  -input /hadoop-mini-project/output/all_accidents \
  -mapper /hadoop-mini-project/autoinc_mapper2.py -file /hadoop-mini-project/autoinc_mapper2.py \
  -reducer /hadoop-mini-project/autoinc_reducer2.py -file /hadoop-mini-project/autoinc_reducer2.py \
  -output /hadoop-mini-project/output/make_year_count

'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast
import os
import time


spark = SparkSession.builder.appName('Optimize I').getOrCreate()
project_path = os.getcwd()

questions_input_path = os.path.join(project_path, 'data/questions')
questionsDF = spark.read.option('path', questions_input_path).load()
'''
86,936 questions
'''
# type(questionsDF)
# print((questionsDF.count(), len(questionsDF.columns)))
# questionsDF.printSchema()
questionsDF.show(10)


answers_input_path = os.path.join(project_path, 'data/answers')
answersDF = spark.read.option('path', answers_input_path).load()
'''
110,714 answers
'''
# type(answersDF)
# print((answersDF.count(), len(answersDF.columns)))
# answersDF.printSchema()
answersDF.show(10)

'''
The query will show the number of answers for each question-month.
PRE-OPTIMIZATION
'''

t0 = time.time()
answers_month = answersDF.withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month') \
    .agg(count('*').alias('cnt'))
resultDF = questionsDF.join(answers_month, 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month') \
    .show()
t1 = time.time()
print(f'{round((t1 - t0)*1000)} ms')
'''
73,020 results
'''
# print(answers_month.count())
# print(resultDF.count())
# resultDF.filter(resultDF.question_id == 155989) \
#     .show()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''

answers_month.explain()
resultDF.explain()
# also look at http://localhost:4040/jobs

'''
POST-OPTIMIZATION
'''

# currently just have a broadcast for slight improvement
t2 = time.time()
answers_month = answersDF.withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month') \
    .agg(count('*').alias('cnt'))
resultDF = questionsDF.join(broadcast(answers_month), 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month') \
    .show()
t3 = time.time()
print(f'{round((t3 - t2)*1000)} ms')
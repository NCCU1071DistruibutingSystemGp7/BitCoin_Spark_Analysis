# -*- coding: utf8 -*-
import datetime
from pyspark import SparkContext
from operator import add
from pyspark.sql import SQLContext, SparkSession
import csv

APP_NAME = 'project'
RECEIVER = 0
VALUE = 1
TIMESTAMP = 2

def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[%s] %s' % (recent_time, message))

def align_timestamp_to_minute(timestamp):
    timestamp = int(timestamp)
    timestamp = timestamp - (timestamp % 60)
    return str(timestamp)

'''
def sum_simultaneous_transaction(rdd):
    rdd = rdd.map(lambda line: ((line[SENDER], line[RECEIVER], align_timestamp_to_minute(line[TIMESTAMP])), float(line[VALUE])))\
        .reduceByKey(add)\
        .map(lambda line: (line[0][0], line[0][1], line[1], line[0][2]))\
        .filter(lambda line: line[VALUE] != 0.0)
    return rdd

def split_send_recv(rdd):
    rdd = rdd.flatMap(lambda line: ((line[SENDER], 1, 0, line[TIMESTAMP]),
                                    (line[RECEIVER], 0, 1, line[TIMESTAMP])))
    return rdd

def reduce_send_recv(rdd):
    rdd = rdd.map(lambda line: ((line[0], line[1], line[2], line[3]),))\
        .distinct()\
        .map(lambda line: line[0])
    return rdd
'''

def Receiver_Count(rdd):
#    rdd = rdd.map(lambda line: (align_timestamp_to_minute(line[TIMESTAMP]), 1))\
    rdd = rdd.map(lambda line: (line[RECEIVER], 1))\
        .reduceByKey(add)
    return rdd

def Time_Count(rdd):
#    rdd = rdd.map(lambda line: (align_timestamp_to_minute(line[TIMESTAMP]), 1))\
    rdd = rdd.map(lambda line: (line[TIMESTAMP], 1))\
        .reduceByKey(add)
    return rdd

def HighestValueRanking(rdd):
    rdd = rdd.map(lambda line: (line[RECEIVER], float(line[VALUE])))\
        .reduceByKey(add)
    return rdd


if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sqlContext = SQLContext(spark)
    logger('Reading blockchain data...')
    #CSV = 'gs://global-unique-name-1/*.csv'
    CSV = './blockchain_cleaned.csv'
    dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
    dataset = Receiver_Count(dataset)
    #df = sqlContext.createDataFrame(dataset, ['Time', 'Count'])
    #df.show()

    result = dataset.collect()
    print(*result, sep='\n')


    with open('result.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(result)

    spark.stop()
    logger('Done!')

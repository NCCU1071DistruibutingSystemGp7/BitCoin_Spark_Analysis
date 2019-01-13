# -*- coding: utf8 -*-
import datetime
from pyspark import SparkContext
from operator import add
from pyspark.sql import SQLContext, SparkSession
import csv
from graphframes import *

APP_NAME = 'project'

def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[%s] %s' % (recent_time, message))

def align_timestamp_to_minute(timestamp):
    timestamp = int(timestamp)
    timestamp = timestamp - (timestamp % 60)
    return str(timestamp)

if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sqlContext = SQLContext(spark)

    logger('Ready to do GraphX...')
    node = spark.read.load('./Fil_Joint.csv', format='csv', header=False).rdd.cache()
    v = sqlContext.createDataFrame(node, ['id','name','value'])
#    df_node.show()
    edge = spark.read.load('./Fil_edge.csv', format='csv', header=False).rdd.cache()
    e = sqlContext.createDataFrame(edge, ["src","dst","relationship"])
#    df_edge.show()
#
    graph = GraphFrame(v,e)
#
    spark.stop()
    logger('Successfully construct a GraphFrame')

# -*- coding: utf8 -*-
import datetime
from pyspark import SparkContext
from operator import add
from pyspark.sql import SQLContext, SparkSession
import csv
from graphframes import *

APP_NAME = 'project'
SENDER = 0
RECEIVER = 1
VALUE = 2

def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[%s] %s' % (recent_time, message))

def align_timestamp_to_minute(timestamp):
    timestamp = int(timestamp)
    timestamp = timestamp - (timestamp % 60)
    return str(timestamp)
def Sender_Count(rdd):
    rdd = rdd.map(lambda line: (line[SENDER], float(line[VALUE])))\
        .reduceByKey(add)
    return rdd

def Receiver_Count(rdd):
    rdd = rdd.map(lambda line: (line[RECEIVER], float(line[VALUE])))\
        .reduceByKey(add)
    return rdd

def Joint_Count(rdd):
    rdd = rdd.reduceByKey(add)
    return rdd

if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sqlContext = SQLContext(spark)
    logger('Reading blockchain data...')
    Sep_edge = './Final_Preprocessing/Fil_edge_'
    Edge_p1  = spark.read.load(Sep_edge+'p1.csv', format='csv', header=False)
    Edge_p2  = spark.read.load(Sep_edge+'p2.csv', format='csv', header=False)
    Edge_p3  = spark.read.load(Sep_edge+'p3.csv', format='csv', header=False)
    Edge_p4  = spark.read.load(Sep_edge+'p4.csv', format='csv', header=False)

    Finalized_Edge = Edge_p1.union(Edge_p2).union(Edge_p3).union(Edge_p4)
    Finalized_Edge.show()

    with open('./Final_Preprocessing/Fil_edge.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(Finalized_Edge.collect())

    Fil_Send = Sender_Count(Finalized_Edge.rdd.cache())
    Fil_Rece = Receiver_Count(Finalized_Edge.rdd.cache())
    df_Send = sqlContext.createDataFrame(Fil_Send, ['txid', 'value'])
    df_Rece = sqlContext.createDataFrame(Fil_Rece, ['txid', 'value'])
   
    Joint = df_Send.union(df_Rece)
    Fil_Joint = Joint_Count(Joint.rdd.cache())
    df_Joint = sqlContext.createDataFrame(Fil_Joint, ['txid', 'value'])
    df_Joint = df_Joint.withColumn('name',df_Joint.txid)
    df_Joint = df_Joint.select(['txid','name','value'])

#    df_Joint.write.csv('Fil_Joint_p1.csv')
    with open('./Final_Preprocessing/Fil_Joint.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(df_Joint.collect())
    
#    Fil_edge.write.csv('Fil_edge_p1.csv')

    spark.stop()
    logger('Done!')

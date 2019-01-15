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

def show_rdd(rdd, num):
    for row in rdd.take(num):
        print(row)

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
    g = GraphFrame(v,e)


    # vertices
    logger('VERTICES')
    print('### vertices ###')
    vDF = g.vertices
    vDF.show(10, False)


    # edges
    logger('EDGES')
    print('### edges ###')
    eDF = g.edges
    eDF.show(10, False)

    print('Edges analysis: transaction value rank op 10')
    eRDD = eDF.rdd
    eRDD = eRDD.sortBy(lambda row: row[2], ascending=False)
    show_rdd(eRDD, 10)


    # indegree
    logger('INDEGREE')
    print('### inDegree ###')
    inDegreeDF = g.inDegrees
    inDegreeDF.show(10, False)

    print('inDegree analysis 1: inDegree rank top 10')
    inDegreeRDD = inDegreeDF.rdd
    inDegreeRDD = inDegreeRDD.sortBy(lambda row: row[1], ascending=False)
    show_rdd(inDegreeRDD, 10)

    print('inDegree analysis 2: value of inDegree rank top 10')
    inDegreeRDD = inDegreeDF.rdd
    inDegreeRDD = inDegreeRDD.map(lambda row: (row[1], 1)).reduceByKey(add).sortBy(lambda row: row[1], ascending=False)
    show_rdd(inDegreeRDD, 10)


    # outdegree
    logger('OUTDEGREE')
    print('### outdegree ###')
    outDegreeDF = g.outDegrees
    outDegreeDF.show(10, False)

    print('outDegree analysis 1: outDegree rank top 10')
    outDegreeRDD = outDegreeDF.rdd
    outDegreeRDD = outDegreeRDD.sortBy(lambda row: row[1], ascending=False)
    showered(outDegreeRDD, 10)

    print('outDegree analysis 2: value of ioutDegree rank top 10')
    outDegreeRDD = outDegreeDF.rdd
    outDegreeRDD = outDegreeRDD.map(lambda row: (row[1], 1)).reduceByKey(add).sortBy(lambda row: row[1], ascending=False)
    show_rdd(outDegreeRDD, 10)


    # PageRank
    logger('PAGERANK')
    print('### PageRank ###')
    pr = g.pageRank(resetProbability=0.15, tol=0.01)

    print('PageRank analysis 1: vertices score (pagerank) rank top/last 10')
    prVerRDD = pr.vertices.rdd
    prVerRDD = prVerRDD.map(lambda row: (round(float(row[3]), 5), 1)).reduceByKey(add).sortBy(lambda row: row[1], ascending=False)
    show_rdd(prVerRDD, 10)

    print('PageRank analysis 2: edges weight rank top 10')
    prEdgRDD = pr.edges.rdd
    prEdgRDD = prEdgRDD.map(lambda row: (round(float(row[3]), 5), 1)).reduceByKey(add).sortBy(lambda row: row[1], ascending=False)
    show_rdd(prEdgRDD, 10)
#
    spark.stop()
    logger('Successfully construct a GraphFrame')

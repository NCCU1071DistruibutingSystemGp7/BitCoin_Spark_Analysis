# -*- coding: utf8 -*-
import datetime
from pyspark import SparkContext
from operator import add
from pyspark.sql import SQLContext, SparkSession
import csv

APP_NAME = 'project'

def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[%s] %s' % (recent_time, message))

def deal_with_sci_rep(number_str):
    index_e = number_str.find('e')
    index_mark = number_str.find('-')
    if index_e == -1:
        return float(number_str)
    else:
        pre = float(number_str[:index_e])
        if index_mark == -1:
            return str(pre * (10 ** int(number_str[index_e+1:])))
        else:
            return str(pre / (10 ** int(number_str[index_mark+1:])))


def highest_value_sender_rank(rdd):
    rdd = rdd.map(lambda row: (row[0], float(row[1]))).sortBy(lambda row: row[1], ascending=False)
    return rdd

def highest_frequency_sender_rank(rdd):
    rdd = rdd.map(lambda row: (row[0], float(row[1]))).sortBy(lambda row: row[1], ascending=False)
    return rdd

def value_interval(rdd):
    rdd = rdd.map(lambda row: (group_value(row[1]), 1)).reduceByKey(add)
    return rdd

def time_interval(rdd):
    rdd = rdd.map(lambda row: (group_timestamp(row[2]), 1)).reduceByKey(add)
    return rdd

def group_value(value):
    value = float(value)
    if value >= 10000:
        return 'value >= 10000'
    if 7500 <= value < 10000:
        return '7500 <= value < 10000'
    if 5000 <= value < 7500:
        return '5000 <= value < 7500'
    if 2500 <= value < 5000:
        return '2500 <= value < 5000'
    if 1000 <= value < 2500:
        return '1000 <= value < 2500'
    if 750 <= value < 1000:
        return '750 <= value < 1000'
    if 500 <= value < 750:
        return '500 <= value < 750'
    if 250 <= value < 500:
        return '250 <= value < 500'
    if 1 <= value < 250:
        return '1 <= value < 250'
    if 0.55 <= value and value < 1:
        return '0.55 <= value < 1'
    if 0.1 <= value and value < 0.55:
        return '0.1 <= value < 0.55'
    if 0.055 <= value < 0.1:
        return '0.055 <= value < 0.1'
    if 0.01 <= value < 0.055:
        return '0.01 <= value < 0.055'
    if 0.0055 <= value < 0.01:
        return '0.0055 <= value < 0.01'
    if 0.001 <= value < 0.0055:
        return '0.001 <= value < 0.0055'
    if 0.00055 <= value < 0.001:
        return '0.00055 <= value < 0.001'
    if 0.0001 <= value < 0.00055:
        return '0.0001 <= value 0.00055'
    if 0.000055 <= value < 0.0001:
        return '0.000055 <= value < 0.0001'
    if 0.00001 <= value < 0.000055:
        return '0.00001 <= value < 0.000055'
    if value < 0.00001:
        return 'value < 0.00001'

def group_timestamp(time):
    time = int(time)
    start_time = 1540317600

    if time < (start_time + 3600 * 1):
        return '10/23/2018 1800-1900'
    if time < (start_time + 3600 * 2):
        return '10/23/2018 1900-2000'
    if time < (start_time + 3600 * 3):
        return '10/23/2018 2000-2100'
    if time < (start_time + 3600 * 4):
        return '10/23/2018 2100-2200'
    if time < (start_time + 3600 * 5):
        return '10/23/2018 2200-2300'
    if time < (start_time + 3600 * 6):
        return '10/23/2018 2300-0000'
    if time < (start_time + 3600 * 7):
        return '10/24/2018 0000-0100'
    if time < (start_time + 3600 * 8):
        return '10/24/2018 0100-0200'
    if time < (start_time + 3600 * 9):
        return '10/24/2018 0200-0300'
    if time < (start_time + 3600 * 10):
        return '10/24/2018 0300-0400'
    if time < (start_time + 3600 * 11):
        return '10/24/2018 0400-0500'
    if time < (start_time + 3600 * 12):
        return '10/24/2018 0500-0600'
    if time < (start_time + 3600 * 13):
        return '10/24/2018 0600-0700'
    if time < (start_time + 3600 * 14):
        return '10/24/2018 0700-0800'
    if time < (start_time + 3600 * 15):
        return '10/24/2018 0800-0900'
    if time < (start_time + 3600 * 16):
        return '10/24/2018 0900-1000'
    if time < (start_time + 3600 * 17):
        return '10/24/2018 1000-1100'
    if time >= (start_time + 3600 * 17):
        return '10/24/2018 1100-1200'

if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sqlContext = SQLContext(spark)
    logger('Reading blockchain data...')

    # highest value sender ranking
#    CSV = './datasets/Highest_Value_Ranking.csv'
#    dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
#    dataset = highest_value_sender_rank(dataset)
#    result = dataset.collect()
#    with open('highest_value_sender_ranking.csv', 'w', newline='') as csvfile:
#        writer = csv.writer(csvfile)
#        writer.writerows(result)


    # highest frequency sender ranking
#    CSV = './datasets/Receiver_Count.csv'
#    dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
#    dataset = highest_frequency_sender_rank(dataset)
#    result = dataset.collect()
#    with open('highest_frequency_sender_ranking.csv', 'w', newline='') as csvfile:
#        writer = csv.writer(csvfile)
#        writer.writerows(result)


    # count value interval
#    CSV = './datasets/blockchain_cleaned.csv'
#    dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
#    dataset = value_interval(dataset)
#    result = dataset.collect()
#    with open('value_interval.csv', 'w', newline='') as csvfile:
#        writer = csv.writer(csvfile)
#        writer.writerows(result)

    # count time interval
    CSV = './datasets/blockchain_cleaned.csv'
    dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
    dataset = time_interval(dataset)
    result = dataset.collect()
    with open('time_interval.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(result)

    logger('Data proccessing done!')

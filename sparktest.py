  # -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark import *
import os

# os.environ['SPARK_HOME']="/usr/local/src/spark"
# os.environ['HADOOP_HOME']="/usr/local/src/hadoop"
# os.environ['PYSPARK_PYTHON']="/home/hadoop/anaconda3/envs/pyspark/bin/python"
# os.environ['PYSPARK_DRIVER_PYTHON']="/home/hadoop/anaconda3/envs/pyspark/bin/python"

if __name__ == '__main__':
    sc = SparkContext("local[8]")
    rdd = sc.parallelize("hello Pyspark world".split(" "))
    counts = rdd \
        .flatMap(lambda line: line) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .foreach(print)
    sc.stop

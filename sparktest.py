  # -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark import *
import os
print(os.environ['SPARK_HOME'])
print(os.environ['HADOOP_HOME'])
if __name__ == '__main__':
    sc = SparkContext("local[8]")
    rdd = sc.parallelize("hello Pyspark world".split(" "))
    counts = rdd \
        .flatMap(lambda line: line) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .foreach(print)
    sc.stop

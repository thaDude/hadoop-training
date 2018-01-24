#!/usr/bin/env python

import sys
import codecs

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("Basic RDD example on LTW data")\
    .getOrCreate()

# FIXME: To make sure we output non-ASCII to stdout in PySpark. Valid for Python 2.x.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


def main(inputs):
    rdd = spark.sparkContext.textFile(inputs)
    result = rdd.map(lambda s: (tuple(s.split('\t')[0:2]), None))\
        .reduceByKey(lambda x, y: None)\
        .map(lambda x: (x[0][0], 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: ((x[1], x[0]), None))\
        .sortByKey(False)\
        .keys()\
        .collect()
    print(result)

if __name__ == '__main__':
    input_files = sys.argv[1]
    main(input_files)

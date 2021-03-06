from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

#===========================================================
# @author Mahmoud Parsian
#===========================================================
#
### The compute_stats function accepts a list of 
### frequencies (as numbers) and computes three values: 
###    1) average 
###    2) median 
###    3) standard deviation
#
import statistics # 1
# frequencies = [number1, number2, ...]
# <1> this module provides functions for calculating mathematical statistics of numeric data
# <2> accept a list of frequencies
# <3> compute average of frequencies
# <4> compute median of frequencies
# <5> compute standard deviation of frequencies
#
def compute_stats(frequencies): # <2>
    average = statistics.mean(frequencies)  # <3>
    median = statistics.median(frequencies) # <4>
    standard_deviation = statistics.stdev(frequencies) # <5>
    return (average, median, standard_deviation) # <6>
#end-def
#===========================================================
#
# record: <url_address><,><frequency>
# <1> accept a record of the form "URL-address,frequency"
# <2> tokenize input record, tokens[0]: URL-address (as key), tokens[1]: frequency (as value)
# <3> return a pair of (URL-address, frequency)
#
def create_pair(record):  # <1>
    tokens = record.split(',')  # <2>
    url_address = tokens[0]
    frequency = int(tokens[1])
    return (url_address, frequency)  # <3>
#end-def
#===========================================================


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: compute_stats.py <file>", file=sys.stderr)
        exit(-1)

    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("compute_stats")\
        .getOrCreate()

    input_path = sys.argv[1]
    print("input path : ", input_path)

    # <1> spark denotes an instance of a SparkSession, the entry point to programming Spark
    # <2> sparkContext (is an attribute of SparkSession), main entry point for Spark functionality
    # <3> read data as distributed set of string records (creates an `RDD[String]`)
    # <4> drop out records, where record size is less than or equal 5 (keep records if their length is greater than 5)
    # <5> create (URL-address, frequency) pairs from a given input record
    # <6> group your data by keys (each key -- as a URL-address -- will be associated with a list of frequencies)
    # <7> apply the `compute_stats()` function to list of frequencies
    
    # read input and create an RDD[String]
    records = spark.sparkContext.textFile(input_path)
    print("records : ", records.collect())
    
    # drop unwanted records
    filtered = records.filter(lambda record: len(record) > 5)
    print("filtered : ", filtered.collect())
    
    # create pairs of (URL-Address, frequency)
    pairs =  filtered.map(create_pair)
    print("pairs : ", pairs.collect())
    
    # GROUP BY URL-Address
    grouped = pairs.groupByKey()
    print("grouped : ", grouped.mapValues(lambda iter : list(iter)).collect())
    
    # compute final results
    results = grouped.mapValues(compute_stats)   
     
    # display final results
    print("results = ", results.collect())

    spark.stop()

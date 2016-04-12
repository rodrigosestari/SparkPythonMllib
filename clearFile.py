'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
import sys
import os
import json
from random import random
from operator import add

from pyspark import SparkContext,SparkConf


if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6"
    
    conf = SparkConf().setAppName("Clear File")
        #setMaster("spark://quickstart.cloudera:7077")
        #.setSparkHome('/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6') \
        #.set('spark.executor.extraClassPath', '/Users/sestari/Documents/mongo-hadoop-core-1.5.2.jar') #;/Users/sestari/Documents/mongo-java-driver-3.0.4.jar;/Users/sestari/Documents/mongodb-driver-3.2.2.jar')

    sc = SparkContext(conf=conf)
   
    log = sc.textFile("log/p.csv")  
    
    def getCategory(requestHttp):        
        return ""
        
    def getLocation(cellPosition):
        return ""
 
        
    
   
    lines = log.flatMap(lambda lines : lines.splitlines());
    

    counts = log.flatMap(lambda line: line.split(";")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda k1, k2:  k1 + k2)
             
    lines.sa
             

    
    sc.stop()
    

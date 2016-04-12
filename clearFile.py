'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import StructField,StringType
import os
import time



if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6"
                                #/usr/hdp/current/spark-client
    
    conf = SparkConf().setAppName("Clear File")
        #setMaster("spark://127.0.0.1:7077")
        #.setSparkHome('/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6') \
        #.set('spark.executor.extraClassPath', '/Users/sestari/Documents/mongo-hadoop-core-1.5.2.jar') #;/Users/sestari/Documents/mongo-java-driver-3.0.4.jar;/Users/sestari/Documents/mongodb-driver-3.2.2.jar')

    sc = SparkContext(conf=conf)
   
    log = sc.textFile("logs/p.csv")  
    
    #function to get the categories through URL
    def getCategory(requestHttp):        
        return requestHttp
        
    #function to get the position through the cell
    def getLocation(cellPosition):
        return ""
 
        
    #it will be used in Spark SQL    
    schemaCSV ='Ip;Resquest;3;4;Url;6;7;8;listOfURI;sequenceId;serviceContainers;12'
    fields = [StructField(field_name, StringType(), True) for field_name in schemaCSV.split(';')]
    
    #get the important fields
    resultMap = log.map(lambda line: line.split(";")).filter(lambda line: len(line)>1).map(lambda row: (row[1], getCategory(row[5]),getLocation(row[6]),row[10] ))
    
    resultMap.saveAsTextFile("result"+str(time.time()))
    #line.saveAsNewAPIHadoopFile("/test.txt", 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat' )

   
    
    ## line =  log.flatMap(lambda line : line.split(";")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()

    sc.stop()
    

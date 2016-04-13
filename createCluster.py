'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
from pyspark.sql import SQLContext
import os




if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6"
                                #/usr/hdp/current/spark-client
    
    conf = SparkConf().setAppName("Spark Cluster")
        #setMaster("spark://127.0.0.1:7077")
        #.setSparkHome('/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6') \
        #.set('spark.executor.extraClassPath', '/Users/sestari/Documents/mongo-hadoop-core-1.5.2.jar') #;/Users/sestari/Documents/mongo-java-driver-3.0.4.jar;/Users/sestari/Documents/mongodb-driver-3.2.2.jar')

    sc = SparkContext(conf=conf)     
    sqlContext = SQLContext(sc)

    logSplited = sc.textFile("result1460566199.47/part-00000").cache()
   
    fields = StructType([StructField("id", IntegerType(), True),StructField("url", StringType(), True),StructField("5", StringType(), True), \
                         StructField("sequenceId", IntegerType(), True),StructField("data", StringType(), True)])
    
    schemaDataFrame= sqlContext.applySchema(logSplited, fields)
  
    
    #function to get the categories through URL
    def getCategory(requestHttp):        
        return requestHttp
        
    #function to get the position through the cell
    def getLocation(cellPosition):
        return ""
 

    
    
    
    
   
    
    sc.stop()
    

'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import StructField,StringType
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

   
    logSplited = sc.textFile("result1460495650.17/part-00000")
    data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")  
    
    #function to get the categories through URL
    def getCategory(requestHttp):        
        return requestHttp
        
    #function to get the position through the cell
    def getLocation(cellPosition):
        return ""
 
        
    #create Data Frame with Spark SQL
    schemaCSV ='Resquest;Url;6;sequenceId'
    fields = [StructField(field_name, StringType(), True) for field_name in schemaCSV.split(';')]
    schemaDataFrame= sqlContext.createDataFrame(logSplited, fields)
    
    
    
    
    
   
    
    sc.stop()
    

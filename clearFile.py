'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import IntegerType,StructField,StructType,StringType,ArrayType,DateType
from pyspark.sql import SQLContext
from datetime import date
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
    sqlContext = SQLContext(sc)
   
    log = sc.textFile("logs/p.csv").cache()
    
    
    #function to get the categories through URL
    def getCategory(requestHttp):        
        #return requestHttp.strip('"').encode('utf-8')
        key =  ["trenitalia","it",["news","sport"]]
        return key
        
    #function to get the position through the cell
    def getLocation(cellPosition):
        return cellPosition.strip('"').encode('utf-8')
    
    
    #function to get the position through the cell
    def getData(datep):
        return date.fromordinal(datep)
    #strftime("%d/%m/%Y").encode('utf-8')
 
  
    
    #get the important fields
    resultMap = log.map(lambda line: line.split(";")).filter(lambda line: len(line)>1). \
                    map(lambda row: (int(row[1]), getCategory(row[4]),int(1),getLocation(row[5]), \
                                     int(row[9]), getData())).repartition(1)
    
      

    #put on Json
    fields = StructType([StructField("requestId", IntegerType(), True),  \
            StructField("url", StructType([StructField("name", StringType(), False),StructField("domain", StringType(), True), \
                                            StructField("categories", ArrayType(StringType(), True), True)]), False), \
            StructField("kind", IntegerType(), True),StructField("location", StringType(), True),  \
            StructField("sequenceId", IntegerType(), True),StructField("data", DateType(), True)])
    schemaDataFrame= sqlContext.applySchema(resultMap, fields)
    data = schemaDataFrame.toJSON()
    data.saveAsTextFile("result"+str(time.time()))
    

    sc.stop()
    

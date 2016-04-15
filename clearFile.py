'''
Created on Apr 12, 2016

@author: sestari
'''
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import IntegerType,StructField,StructType,StringType,ArrayType,TimestampType
from pyspark.sql import SQLContext
from dateutil.parser import parse
import time
import os
import re



if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6"
                                #/usr/hdp/current/spark-client
    
    conf = SparkConf().setAppName("Clear File")
        #setMaster("spark://127.0.0.1:7077")
        #.setSparkHome('/Users/sestari/Documents/spark-1.6.1-bin-hadoop2.6') \
        #.set('spark.executor.extraClassPath', '/Users/sestari/Documents/mongo-hadoop-core-1.5.2.jar') #;/Users/sestari/Documents/mongo-java-driver-3.0.4.jar;/Users/sestari/Documents/mongodb-driver-3.2.2.jar')

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
   
    log = sc.textFile("logs/new.csv").cache()
    
    
    #function to get the categories through URL
    def getCategory(requestHttp):                
        key =  [["trenitalia","it",["train","buy"]], ["gazzetta","it",["news","sport"]]]
        return key
        
    
    
    #function to get the position through the cell
    def getData(datep):
        return parse(datep)
    



    def regularExpressionFilter(url):
        matchObj = re.search('ads.g', url)
        match1bj = re.search('porn', url)
        match2bj = re.search('XXL', url)
        match3bj = re.search('.xvideo', url)        
        match4bj = re.search('fuck', url)
        match5bj = re.search('.jpg', url)

        if matchObj or match1bj or match2bj or match3bj or match4bj or match5bj:
            return False
        else:
            return True

    def regularExpression(httprequest):
        size =len(httprequest)    
        if size > 1:
           httprequest[0] =  httprequest[0][1:]
           httprequest[size-1] = httprequest[size-1][:-1]
        array = []
        for item in httprequest:
           if regularExpressionFilter(item):
                array.append(item) 
        return array
       
  
    
    #get the important fields
    resultMap = log.map(lambda line: line.split(";")).filter(lambda line: len(line)>1). \
                    map(lambda row: (str(row[0]), \
                                     int(row[1]), \
                                     int(row[2]), \
                                     getData(row[3]), \
                                     int(row[4]), \
                                     str(row[5]), \
                                     int(row[6]), \
                                     int(row[7]), \
                                     int(row[8]), \
                                     int(row[9]), \
                                     row[10]))
                    
    resultMap_FilterUlr =  resultMap.map(lambda (a,b,c,d,e,f,g,h,i,j,l): (a,b,c,d,e,f,g,h,i,j,regularExpression(l.split(",")))). \
                                    filter(lambda (a,b,c,d,e,f,g,h,i,j,l): len(l) >1)
     
    #put on Json
    fields = StructType( \
                        [StructField("GSN", StringType(), False),  \
                        StructField("ChargingID", IntegerType(), False),  \
                        StructField("RecordSequence", IntegerType(), False),  \
                        StructField("RecordOpeningDate", TimestampType(), False),  \
                        StructField("rATType", IntegerType(), False),  \
                        StructField("UserLocation", StringType(), False),  \
                        StructField("Accuracy", IntegerType(), False),  \
                        StructField("BrowsingSession", IntegerType(), False),  \
                        StructField("Uplink", IntegerType(), False),  \
                        StructField("Downlink", IntegerType(), False), \
                        StructField("Urls", ArrayType(StringType(),False))])
    
     #The new Json Format
    newStructure = StructType( \
                        [StructField("GSN", StringType(), False),  \
                        StructField("ChargingID", IntegerType(), False),  \
                        StructField("RecordSequence", IntegerType(), False),  \
                        StructField("RecordOpeningDate", TimestampType(), False),  \
                        StructField("rATType", IntegerType(), False),  \
                        StructField("UserLocation", StringType(), False),  \
                        StructField("Accuracy", IntegerType(), False),  \
                        StructField("BrowsingSession", IntegerType(), False),  \
                        StructField("Uplink", IntegerType(), False),  \
                        StructField("Downlink", IntegerType(), False), \
                        StructField("Urls",ArrayType( \
                                                     StructType([StructField("name", StringType(), False),StructField("domain", StringType(), True), \
                                                                    StructField("categories", ArrayType(StringType(), True), True)]), False), \
                                                True)])
            
        #get the important fields
    resultMap_newFormat = log.map(lambda line: line.split(";")).filter(lambda line: len(line)>1). \
                                 map(lambda row: (str(row[0]), \
                                     int(row[1]), \
                                     int(row[2]), \
                                     getData(row[3]), \
                                     int(row[4]), \
                                     str(row[5]), \
                                     int(row[6]), \
                                     int(row[7]), \
                                     int(row[8]), \
                                     int(row[9]), \
                                     getCategory(row[9])))
                    
     
    schemaDataFrame= sqlContext.applySchema(resultMap_newFormat, newStructure)
    #schemaDataFrame= sqlContext.applySchema(resultMap_FilterUlr, fields)
    data = schemaDataFrame.toJSON()
    data.saveAsTextFile("result"+str(time.time()))
    

    sc.stop()
    

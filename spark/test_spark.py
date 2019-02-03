import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

import gdelt_schema
import configparser


if __name__ == "__main__":



#    spark = SparkSession.builder \
#    	.appName("GDELT-News") \
#    	.config("spark.executor.memory", "1gb") \
#    	.getOrCreate()

    conf = (SparkConf().setMaster("spark://ec2-18-235-99-234.compute-1.amazonaws.com:7077").setAppName("GDELT-News").set("spark.executor.memory", "6gb"))

    sc = SparkContext(conf = conf) #appName="GDELT-News")
    sqlContext = SQLContext(sc)
    gdelt_bucket = "s3n://gdelt-open-data/v2/events/2019*.export.csv" #	20180410190000.export.csv"
    #gdelt_bucket = "events.csv"

    df = sqlContext.read \
    	.format('com.databricks.spark.csv') \
    	.options(header='false') \
    	.options(delimiter="\t") \
		.load(gdelt_bucket, schema = gdelt_schema.gdeltSchema)

    sqlContext.registerDataFrameAsTable(df, 'temp')

    df_clean = sqlContext.sql("""SELECT GLOBALEVENTID,
                              CAST(SQLDATE AS INTEGER),
                              Actor1Name,
                              SOURCEURL
                            from temp
                            """)

    df_news = df_clean.select('GLOBALEVENTID','SQLDATE','Actor1Name', 'SOURCEURL')

    #df_news.repartition(1000, 'GLOBALEVENTID')


    df_news.show(df_news.count())


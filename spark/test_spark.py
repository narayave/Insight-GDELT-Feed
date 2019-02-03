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



    sc = SparkSession.builder \
	.master("spark://ec2-18-235-99-234.compute-1.amazonaws.com:7077") \
	.appName("GDELT-News") \
	.config("spark.executor.memory", "2gb") \
	.getOrCreate()

    sqlcontext = SQLContext(sc)
    gdelt_bucket = "s3n://gdelt-open-data/v2/events/20180410190000.export.csv"

    df = sqlcontext.read \
    	.format('com.databricks.spark.csv') \
    	.options(header='false') \
    	.options(delimiter="\t") \
	.load(gdelt_bucket, schema = gdelt_schema.gdeltSchema)

    sqlcontext.registerDataFrameAsTable(df, 'temp')

    df_clean = sqlcontext.sql("""SELECT GLOBALEVENTID,
                              CAST(SQLDATE AS INTEGER),
                              Actor1Name,
                              SOURCEURL
                            from temp
                            """)

    df_news = df_clean.select('GLOBALEVENTID','SQLDATE','Actor1Name', 'SOURCEURL')

    #df_news.repartition(1000, 'GLOBALEVENTID')
    print 'I did stuff'

    df_news = df_news.rdd.map(lambda line: (line[1], 1)).reduceByKey(lambda a, b: a + b)

    res = df_news.collect()

    for val in res:
	print val

    #df_news.show(df_news.count())


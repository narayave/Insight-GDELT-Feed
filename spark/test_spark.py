import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import functions as F

import gdelt_schema
import configparser


if __name__ == "__main__":

    sc = SparkSession.builder \
        .master("spark://ec2-54-84-194-198.compute-1.amazonaws.com:7077") \
        .appName("GDELT-News") \
        .config("spark.executor.memory", "2gb").getOrCreate()

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
                              EventCode,
                              Actor1Code,
                              Actor1Name,
                              Actor1Type1Code,
                              ActionGeo_FullName,
                              ActionGeo_CountryCode,
			      Actor1Geo_CountryCode,
                              ActionGeo_ADM1Code,
			      Actor2Name,
			      Actor2Geo_CountryCode,
                              GoldsteinScale,
			      AvgTone
                            from temp
                            """)

    df_news = df_clean.select('GLOBALEVENTID','SQLDATE','EventCode', 'Actor1Code','Actor1Type1Code','ActionGeo_FullName','ActionGeo_CountryCode','ActionGeo_ADM1Code','Actor1Geo_CountryCode','GoldsteinScale', 'AvgTone')

    #df_news.repartition(1000, 'GLOBALEVENTID')
    df_news.show(10)

    df_news = df_news.filter(df_news.ActionGeo_CountryCode == 'US')
    df_news = df_news.filter(df_news.Actor1Code != 'null')

    #df_news = df_news.groupby('ActionGeo_FullName', 'EventCode').agg(F.collect_list(''))
    df_news = df_news.groupby('ActionGeo_CountryCode', 'Actor1Type1Code','ActionGeo_ADM1Code')
    print df_news.count().show()
    #df_news = df_news.rdd.map(lambda)

    # Dataframe object does not have the map attribute, which is why rdd needs to be
    # 	called firstd..reduceByKey(lambda a, b: a + b)
    df_news = df_news.rdd.map(lambda line: (line[2], 1)).reduceByKey(lambda a, b: a + b)

    res = df_news.collect()

    for val in res:
	print val

    #df_news.show(df_news.count())


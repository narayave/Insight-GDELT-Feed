import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import array, lit

import gdelt_schema_v1
import gdelt_schema_v2
from six.moves import configparser

def postgres_dump(config, data_frame):

    config.read('/home/ubuntu/Insight-GDELT-Feed/spark/config.ini')
    dbname = config.get('dbauth', 'dbname')
    dbuser = config.get('dbauth', 'user')
    dbpass = config.get('dbauth', 'password')
    dbhost = config.get('dbauth', 'host')
    dbport = config.get('dbauth', 'port')

    url = "jdbc:postgresql://"+dbhost+":"+dbport+"/"+dbname
    properties = {
                "driver": "org.postgresql.Driver",
                "user": dbuser,
                "password": dbpass
                }

    mode = 'append'
    data_frame.write.jdbc(url=url, table="final_results_test", mode=mode, properties=properties)


if __name__ == "__main__":

    config = configparser.ConfigParser()

    sc = SparkSession.builder \
        .master("spark://ec2-54-84-194-198.compute-1.amazonaws.com:7077") \
        .appName("GDELT-News") \
        .config("spark.executor.memory", "5gb") \
        .config("spark.jars", "/home/ubuntu/Insight-GDELT-Feed/spark/postgresql-42.2.5.jar").getOrCreate()

    sqlcontext = SQLContext(sc)
    gdelt_bucket = "s3n://gdelt-open-data/v2/events/20180410190000.export.csv"
    gdelt_bucket2 = "s3n://gdelt-open-data/events/200601.csv"
    #gdelt_bucket = "s3n://gdelt-open-data/v2/events/20190206*.export.csv"
    #gdelt_bucket = "s3n://gdelt-open-data/v2/events/201*.export.csv"

    df = sqlcontext.read \
    	.format('com.databricks.spark.csv') \
    	.options(header='false') \
    	.options(delimiter="\t") \
	.load(gdelt_bucket, schema = gdelt_schema_v2.gdeltSchema) \
 
    df_2 = sqlcontext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter="\t") \
        .load(gdelt_bucket2, schema = gdelt_schema_v1.gdeltSchema)

    sqlcontext.registerDataFrameAsTable(df, 'temp')
    sqlcontext.registerDataFrameAsTable(df_2, 'temp2')

    df_clean = sqlcontext.sql("""SELECT GLOBALEVENTID,
                              CAST(SQLDATE AS INTEGER),
                              MonthYear,
                              Year,
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
                              GoldsteinScale
                            from temp
                            """)

    df_news = df_clean.select('GLOBALEVENTID','SQLDATE','MonthYear','Year','Actor1Code',
                            'Actor1Type1Code','ActionGeo_FullName','ActionGeo_CountryCode',
                            'ActionGeo_ADM1Code','Actor1Geo_CountryCode','GoldsteinScale')

    df_clean2 = sqlcontext.sql("""SELECT GLOBALEVENTID,
                            CAST(SQLDATE AS INTEGER),
                            MonthYear,
                            Year,
                            EventCode,
                            Actor1Code,
                            Actor1Name,
                            Actor1Type1Code,
                            ActionGeo_FullName,
                            ActionGeo_CountryCode,
                            Actor1Geo_CountryCode,
                            ActionGeo_ADM1Code,
                            GoldsteinScale
                            from temp2
                            """)

    df_clean2.show()

    df_news2 = df_clean2.select('GLOBALEVENTID','SQLDATE','MonthYear','Year','Actor1Code',
                            'Actor1Type1Code', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
                            'ActionGeo_ADM1Code','Actor1Geo_CountryCode','GoldsteinScale')


    df_news = df_news.union(df_news2)

    df_news.show()

                                    
    role_codes = ["COP", "GOV", "JUD", "BUS", "CRM", "DEV", "EDU", "ENV" \
                    "HLH", "LEG", "MED", "MNC"]

    df_news = df_news.filter(df_news.ActionGeo_CountryCode == 'US')
    df_news = df_news.filter(df_news.Actor1Code != 'null')
    df_news = df_news.filter(df_news.Actor1Type1Code.isin(role_codes))

    name = 'ActionGeo_ADM1Code'
    udf = UserDefinedFunction(lambda x: x[:2]+'-'+x[2:], StringType())
    df_news = df_news.select(*[udf(column).alias(name) if column == name else column for column in df_news.columns])

    name = 'GoldsteinScale'
    min_scale, max_scale = -10.0, 10.0
    norm = UserDefinedFunction(lambda x: (x - min_scale)/(max_scale - min_scale), DoubleType())
    df_news = df_news.select(*[norm(col).cast(DoubleType()).alias('normg_scale') if col == name else col for col in df_news.columns])


    split_col = F.split(df_news['ActionGeo_ADM1Code'],'-')
    df_news = df_news.withColumn('action_state', split_col.getItem(1))
    print df_news.show()

    df_news = df_news.filter(df_news.action_state != '')

    df_news = df_news.groupby('action_state','Year','Actor1Type1Code').agg( F.approx_count_distinct('GLOBALEVENTID').alias('events_count'),
                                                                            #F.col('SQLDATE'),
                                                                            #F.collect_list('EventCode'),
                                                                            #F,avg('AvgTone').alias('avg_tone'),
                                                                            F.sum('normg_scale').alias('norm_score_cale'))
                                                                            #Calculate avg by dividing by event count

    print df_news.show(df_news.count())
    print df_news.printSchema()

    #postgres_dump(config, df_news)



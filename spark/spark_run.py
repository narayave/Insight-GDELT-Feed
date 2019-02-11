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
    data_frame.write.jdbc(url=url, table="central_results", mode=mode, \
                properties=properties)


def get_clean_df(sqlcontext, sql_df):

    sqlcontext.registerDataFrameAsTable(sql_df, 'temp')

    # export PYTHONIOENCODING=utf8 <- ran that for utf8 decode error

    df_clean = sqlcontext.sql("""SELECT GLOBALEVENTID,
                        CAST(SQLDATE AS INTEGER), MonthYear,
                        Year, EventCode, Actor1Code, Actor1Name,
                        Actor1Type1Code, ActionGeo_FullName,
                        ActionGeo_CountryCode, Actor1Geo_CountryCode,
                        ActionGeo_ADM1Code, Actor2Name,
                        Actor2Geo_CountryCode,
                        CAST(GoldsteinScale AS DOUBLE)
                        from temp
                        """)

    df_select = df_clean.select('GLOBALEVENTID','SQLDATE','Year','Actor1Code',
                                'Actor1Type1Code','ActionGeo_FullName',
                                'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
                                'Actor1Geo_CountryCode','GoldsteinScale')

    return df_select


def filter_df(df_news):
    role_codes_of_interest = ["COP", "GOV", "JUD", "BUS", "CRM", "DEV", "EDU", "ENV" \
                                "HLH", "LEG", "MED", "MNC"]

    # Filter data records that are of interest
    df_news = df_news.filter(df_news.ActionGeo_CountryCode == 'US')
    df_news = df_news.filter(df_news.Actor1Code != 'null')
    df_news = df_news.filter(df_news.Actor1Type1Code.isin(role_codes_of_interest))

    df_news.show()

    #df_news = df_news.filter(df_news.GoldsteinScale != 'null')
    #df_news = df_news.filter(df_news.GoldsteinScale != '')

    # There isn't a specific column specifying the state that the event happens in
    #   The state is tied with the country code
    #   So, parse out the state and make it a new column
    name = 'ActionGeo_ADM1Code'
    udf = UserDefinedFunction(lambda x: x[:2]+'-'+x[2:], StringType())
    df_news = df_news.select(*[udf(column).alias(name) if column == name else \
                                column for column in df_news.columns])
    split_col = F.split(df_news['ActionGeo_ADM1Code'],'-')
    df_news = df_news.withColumn('action_state', split_col.getItem(1))

    # The GoldsteinScale's range is between -10 and 10, but it's mostly sparse
    #   It's hard for a user to make use of it
    #   The scale is normalized and served as a scale between 0 to 100.
    #   Higher value denotes a more positive outcome
    name = 'GoldsteinScale'
    min_scale, max_scale = -10.0, 10.0
    norm = UserDefinedFunction(lambda x: (x - min_scale)/(max_scale - min_scale), DoubleType())
    df_news = df_news.select(*[norm(col).cast(DoubleType()) \
                .alias('normg_scale') if col == name else col for col in \
                    df_news.columns])

    df_news = df_news.filter(df_news.action_state != '')

    df_news = aggregate_job(df_news)

    return df_news

def aggregate_job(df_news):

    df_finalized = df_news.groupby('action_state','Year','Actor1Type1Code') \
                    .agg( \
                        F.approx_count_distinct('GLOBALEVENTID').alias('events_count'),
                        F.sum('normg_scale').alias('norm_score_cale'))
                        #Calculate avg by dividing by event count

    print df_finalized.show(df_finalized.count())
    #print df_finalized.printSchema()

    #if not 'Actor1Type1Code' in df_finalized.columns:
    #   df = df.withColumn('Actor1Type1Code', f.lit(''))

    return df_finalized

if __name__ == "__main__":

    config = configparser.ConfigParser()

    sc = SparkSession.builder \
        .master("spark://ec2-54-84-194-198.compute-1.amazonaws.com:7077") \
        .appName("GDELT-News") \
        .config("spark.executor.memory", "5gb") \
        .config("spark.jars", "/home/ubuntu/Insight-GDELT-Feed/spark/postgresql-42.2.5.jar").getOrCreate()

    sqlcontext = SQLContext(sc)


    #gdelt_bucket1 = "s3n://gdelt-open-data/events/[1,2][0-9]*[0-9][0-3|9].csv"
    #gdelt_bucket2 = "s3n://gdelt-open-data/events/*.export.csv"
    gdelt_bucket1 = "s3n://gdelt-open-data/events/200806.csv"
    gdelt_bucket2 = "s3n://gdelt-open-data/events/20150329.export.csv"

    # Original datasets had a different schema from the new one
    #   Thus, we need two different ways to read the data
    df_1 = sqlcontext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter="\t") \
        .load(gdelt_bucket1, schema = gdelt_schema_v1.gdeltSchema)

    df_2 = sqlcontext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter="\t") \
        .load(gdelt_bucket2, schema = gdelt_schema_v2.gdeltSchema)


    # Put together 2 sets of data with different schemas
    df_v1 = get_clean_df(sqlcontext, df_1)
    df_v2 = get_clean_df(sqlcontext, df_2)
    df_news = df_v1.union(df_v2)
    df_news.show()

    # Filter and aggregate data
    processed_df = filter_df(df_news)
    #finalized_df = aggregate_job(processed_df)

    processed_df.printSchema()

    # Write to database
    postgres_dump(config, processed_df)

    print 'Done'

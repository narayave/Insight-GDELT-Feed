import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

import gdelt_schema

if __name__ == "__main__":

    sc = SparkContext(appName="GDELT-News")
    sqlContext = SQLContext(sc)
    #gdelt_bucket = "s3://gdelt-open-data/v2/events/20180410190000.export.csv"
    gdelt_bucket = "events.csv"

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

    df_news.show(df_news.count())


# import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

# # import cassandra spark connector(datastax)
# from cassandra.cluster import Cluster
# from cassandra.query import BatchStatement
# from cassandra import ConsistencyLevel

import gdelt_schema
import config



# def sendCassandra(iter):

#    print("send to cassandra")
#    cluster = Cluster(config.CASSANDRA_SERVER)
#    session = cluster.connect(config.CASSANDRA_NAMESPACE)

#    insert_statement = session.prepare("INSERT INTO  (userid, newsid, count) VALUES (?, ?, ?)")

#    count = 0

    # batch insert into cassandra database
#    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    
#    for record in iter:
#        batch.add(insert_statement, (record['userid'], record['newsid'], record['count']))


        # split the batch, so that the batch will not exceed the size limit
#        count += 1
#        if count % 500 == 0:
#            session.execute(batch)
#           batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    # send the batch that is less than 500            
#    session.execute(batch)
#    session.shutdown()

def reduce_category(cate):
		
	if cate:
		reduced_cate = config.cateMapping[cate]
	else:
		reduced_cate = cate
	
	return reduced_cate

def quiet_logs( sc ): 
	logger = sc._jvm.org.apache.log4j 
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR ) 
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )



if __name__ == "__main__":

	# Set Spark context
	sc = SparkContext(appName="GDELT-News")
	# sc.addPyFile('config.py')
	# # Set Spark SQL context
    # 	sqlContext = SQLContext(sc)

	sqlContext = SQLContext(sc)

	# Read news data from S3
	gdelt_bucket = "s3://gdelt-open-data/v2/events/*.export.csv"

	df = sqlContext.read \
    	.format('com.databricks.spark.csv') \
    	.options(header='false') \
    	.options(delimiter="\t") \
		.load(gdelt_bucket, schema = gdelt_schema.gdeltSchema)
	
	sqlContext.registerDataFrameAsTable(df, 'temp')

	df_clean = sqlContext.sql("""SELECT GLOBALEVENTID,
                              CAST(SQLDATE AS INTEGER), 
                               Actor1Type1Code,
                              CAST(NumMentions AS INTEGER),
							   SOURCEURL
                          	FROM temp
				WHERE Actor1Type1Code <> '' AND Actor1Type1Code IS NOT NULL
                         	""")
	
	sqlContext.dropTempTable('temp')

	df_news = df_clean.select('GLOBALEVENTID','SQLDATE','Actor1Type1Code', 'NumMentions','SOURCEURL')

	df_news.show(20)

	cateReduction = udf(lambda z: reduce_category(z), StringType())

	df_reduced = df_news.select('GLOBALEVENTID','SQLDATE','Actor1Type1Code', 'NumMentions',\
				cateReduction('Actor1Type1Code').alias('category'),'SOURCEURL')

	df_filtered = df_reduced.select(col('GLOBALEVENTID').alias('newsid'),col('SQLDATE').alias('date'),'category',\
	col('NumMentions').alias('mentions'), col('SOURCEURL').alias('sourceurl'))

	df_filtered.repartition(10000, 'newsid')
		
	# # Saving news records to Cassandra
	# df_filtered.write.format("org.apache.spark.sql.cassandra").mode('append').options(
  	# table='news', keyspace=config.CASSANDRA_NAMESPACE).save()

	# user_bucket = "s3a://userclicklogs/log*.txt"
	# # Read user clicklogs from S3
	# click_logs = sc.textFile(user_bucket)
	
	# # Split lines into columns by delimiter '\t'
	# record = click_logs.map(lambda x: x.split("\t"))

	# # Convert Rdd into DataFram
	# df_click = sqlContext.createDataFrame(record,['unixtime','userid','newsid'])

	# df_click.repartition(10000, 'newsid')
	
	# df_join = df_filtered.join(df_click, on='newsid')

	# df_grouped = df_join.groupby(['userid', 'category']).count()

	# df_grouped.write.format("org.apache.spark.sql.cassandra").mode('append').options(
  	# table='summary', keyspace=config.CASSANDRA_NAMESPACE).save()

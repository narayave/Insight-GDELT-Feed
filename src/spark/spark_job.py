from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import UserDefinedFunction


class SparkJob(object):

    def __init__(self):

        self.__session = SparkSession.builder \
            .master("spark://ec2-54-84-194-198.compute-1.amazonaws.com:7077") \
            .appName("GDELT-News") \
            .config("spark.executor.memory", "5gb") \
            .config("spark.jars",
                    "/home/ubuntu/Insight-GDELT-Feed/src/spark/postgresql-42.2.5.jar") \
            .getOrCreate()

        self.sqlcontext = SQLContext(self.__session)
        self.gdelt_bucket1 = "s3n://gdelt-open-data/events/[1,2][0-9]*[0-9][0-3|9].csv"
        self.gdelt_bucket2 = "s3n://gdelt-open-data/events/*.export.csv"

    def sql_read(self, df, schema):

        dataframe = self.sqlcontext.read \
            .format('com.databricks.spark.csv') \
            .options(header='false') \
            .options(delimiter="\t") \
            .load(df, schema=schema)

        return dataframe

    def get_clean_df(self, df):

        self.sqlcontext.registerDataFrameAsTable(df, 'temp')

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

        df_select = df_clean.select('GLOBALEVENTID', 'SQLDATE', 'Year',
                                    'Actor1Code', 'Actor1Type1Code',
                                    'ActionGeo_FullName',
                                    'ActionGeo_CountryCode',
                                    'ActionGeo_ADM1Code',
                                    'Actor1Geo_CountryCode',
                                    'GoldsteinScale')

        return df_select

    def combine_dataframes(self, df1, df2):

        df = df1.union(df2)

        return df

    def filter_df(self, df_news):
        role_codes_of_interest = ["COP", "GOV", "JUD", "BUS", "CRM", "DEV", "EDU",
                                  "ENV", "HLH", "LEG", "MED", "MNC"]

        # Filter data records that are of interest
        df_news = df_news.filter(df_news.ActionGeo_CountryCode == 'US')
        df_news = df_news.filter(df_news.Actor1Code != 'null')
        df_news = df_news.filter(
            df_news.Actor1Type1Code.isin(role_codes_of_interest))

        df_news.show()

        return df_news

    def add_state_column(self, df_news):
        # There isn't a specific column specifying the state that the event happens in
        #   The state is tied with the country code
        #   So, parse out the state and make it a new column
        name = 'ActionGeo_ADM1Code'
        udf = UserDefinedFunction(lambda x: x[:2]+'-'+x[2:], StringType())
        df_news = df_news.select(*[udf(column).alias(name) if column == name
                                   else column for column in df_news.columns])
        split_col = F.split(df_news['ActionGeo_ADM1Code'], '-')
        df_news = df_news.withColumn('action_state', split_col.getItem(1))

        return df_news

    def normalize_goldstein(self, df_news):

        # The GoldsteinScale's range is between -10 and 10, but it's mostly sparse
        # It's hard for a user to make use of it
        # The scale is normalized and served as a scale between 0 to 100.
        # Higher value denotes a more positive outcome
        name = 'GoldsteinScale'
        min_scale, max_scale = -10.0, 10.0
        norm = UserDefinedFunction(lambda x: (
            x - min_scale)/(max_scale - min_scale), DoubleType())
        df_news = df_news.select(*[norm(col).cast(DoubleType())
                                   .alias('normg_scale') if col == name else col for col in
                                   df_news.columns])

        df_news = df_news.filter(df_news.action_state != '')

    def aggregate_job(self, df_news):

        df_finalized = df_news.groupby('action_state', 'Year', 'Actor1Type1Code') \
            .agg(
            F.approx_count_distinct(
                'GLOBALEVENTID').alias('events_count'),
            F.sum('normg_scale').alias('norm_score_cale'))
        # Calculate avg by dividing by event count

        print df_finalized.show(df_finalized.count())
        #print df_finalized.printSchema()

        return df_finalized

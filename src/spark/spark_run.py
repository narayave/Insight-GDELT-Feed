from database_operations import DatabaseOperations
from spark_job import SparkJob
import gdelt_schema_v1
import gdelt_schema_v2


if __name__ == "__main__":

    sparkjob = SparkJob()
    dataops = DatabaseOperations()

    # Original datasets had a different schema from the new one
    #   Thus, we need two different ways to read the data
    df_1 = sparkjob.sql_read(sparkjob.gdelt_bucket1,
                             gdelt_schema_v1.gdeltSchema)
    df_2 = sparkjob.sql_read(sparkjob.gdelt_bucket2,
                             gdelt_schema_v2.gdeltSchema)

    # Put together 2 sets of data with different schemas
    df_1 = sparkjob.get_clean_df(df_1)
    df_2 = sparkjob.get_clean_df(df_2)
    df_news = sparkjob.combine_dataframes(df_1, df_2)
    #df_news.show()

    # Filter
    df_news = sparkjob.filter_df(df_news)

    # Parse and add state column
    df_news = sparkjob.add_state_column(df_news)

    # Normalize goldstein scale
    df_news = sparkjob.normalize_goldstein(df_news)
    print df_news.count()

    # Aggregate work
    finalized_df = sparkjob.aggregate_job(df_news)

    finalized_df.printSchema()

    # Write to database
    dataops.set_table_name("central_results")
    dataops.db_write(finalized_df)

    print 'Done'

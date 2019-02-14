from six.moves import configparser
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pandasql as ps
from pprint import pprint


def get_df(con):

    print 'In get df function'

    sql_query = """
            SELECT GLOBALEVENTID, CAST(SQLDATE AS INTEGER),
            MonthYear, Year, Actor1Code, Actor1Type1Code,
            ActionGeo_FullName, ActionGeo_ADM1Code,
            Actor1Geo_CountryCode, CAST(GoldsteinScale AS FLOAT)
            FROM events
            WHERE Actor1Geo_CountryCode='US' and
                        Actor1Code != 'null' and
                        sqldate >= 20190213 and
                        Actor1Type1Code in ('COP', 'GOV', 'JUD', 'BUS',
                                            'CRM', 'DEV', 'EDU', 'ENV',
                                            'HLH', 'LEG','MED','MNC');
        """

    try:
        dataframe=pd.read_sql_query(sql_query, con)
    except Exception as e:
        print 'Error -' + str(e)

    print dataframe

    return dataframe


def get_clean_df(dataframe):

    query = """
                SELECT GLOBALEVENTID, CAST(SQLDATE AS INTEGER),
                MonthYear, Year, Actor1Code, Actor1Type1Code,
                ActionGeo_FullName, ActionGeo_ADM1Code,
                Actor1Geo_CountryCode, CAST(GoldsteinScale AS DOUBLE)
                FROM dataframe
            """

    print 'In get clean df function'

    results_tmp = ps.sqldf(query, locals())
    print results_tmp

    return df


def filter_dataframe(df):

    print 'In filter df function'

    query = """
            SELECT * FROM df
            WHERE Actor1Geo_CountryCode='US' and Actor1Code != 'null'
    """

    results = ps.sqldf(query, locals())

    pprint(results)

    return results


def get_states(df):


    print df['ActionGeo_ADM1Code']

    return df

def aggregate_data(df):

    print 'In aggregate data function'



if __name__ == '__main__':

    config = configparser.ConfigParser()
    # TODO: Make sure to read the correct config.ini file on AWS workers
    config.read('/home/vee/repos/Insight-GDELT-Feed/gdelt/config.ini')
    dbname = config.get('dbauth', 'dbname')
    dbuser = config.get('dbauth', 'user')
    dbpass = config.get('dbauth', 'password')
    dbhost = config.get('dbauth', 'host')
    dbport = config.get('dbauth', 'port')

    db = create_engine('postgres://%s%s/%s'%(dbuser,dbhost,dbname))
    con = None
    con = psycopg2.connect(database = dbname, host = dbhost, user = dbuser, password = dbpass)

    df = get_df(con)
    # df = get_clean_df(df)

    # df = filter_dataframe(df)

    # df = get_states(df)


    print 'Done'
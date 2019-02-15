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
                        sqldate >= 20190215 and
                        Actor1Type1Code in ('COP', 'GOV', 'JUD', 'BUS',
                                            'CRM', 'DEV', 'EDU', 'ENV',
                                            'HLH', 'LEG','MED','MNC');
        """

    try:
        dataframe=pd.read_sql_query(sql_query, con)
    except Exception as e:
        print 'Error -' + str(e)

    # print dataframe

    return dataframe


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

    action_state = df['actiongeo_adm1code'].apply(lambda x: x[2:])

    df['action_state'] = action_state
    # pprint(df)
    df = df.loc[df.actiongeo_adm1code != "US"]

    # pprint(df)

    return df


def normalize_goldstein(df):

    print 'In normalize goldstein function'

    df = df.loc[df.goldsteinscale > -20.0]

    min_scale, max_scale = -10.000005, 10.000005
    norm_gold = df['goldsteinscale'].apply(lambda x: (x - min_scale)/(max_scale - min_scale))

    print norm_gold

    df['norm_scale'] = norm_gold
    pprint(df)

    return df



def aggregate_data(df):

    print 'In aggregate data function'

    # df = df.groupby(['action_state', 'year', 'actor1type1code'])
    df = df.groupby(['action_state', 'year', 'actor1type1code']).count()
    pprint(df)

    df = df.sum()

    return df


def clean_df(df):

    df = df.drop(['actor1code','actiongeo_fullname', 'actiongeo_adm1code',
                    'actor1geo_countrycode'], axis=1)

    pprint(df)

    return df


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
    df = get_states(df)
    df = normalize_goldstein(df)
    df = clean_df(df)
    df = aggregate_data(df)

    print 'Done'
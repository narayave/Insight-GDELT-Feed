import re

from sqlalchemy import create_engine
from six.moves import configparser
import pandas as pd
import pandasql as ps
from pprint import pprint
import psycopg2

from data_gatherer import DataGatherer


def initial_df_clean(dataframe):

    print 'In the initial df clean function'

    sql_query = """
                SELECT GLOBALEVENTID, CAST(SQLDATE AS INTEGER),
                MonthYear, Year, Actor1Code, Actor1Type1Code,
                ActionGeo_FullName, ActionGeo_ADM1Code,
                Actor1Geo_CountryCode, CAST(GoldsteinScale AS FLOAT)
                FROM dataframe
                WHERE Actor1Geo_CountryCode='US' and
                Actor1Code != 'null' and
                Actor1Type1Code in ('COP', 'GOV', 'JUD', 'BUS',
                                    'CRM', 'DEV', 'EDU', 'ENV',
                                    'HLH', 'LEG','MED','MNC');
                """

    df = ps.sqldf(sql_query, locals())

    return df


def get_states(df):

    action_state = df['ActionGeo_ADM1Code'].apply(lambda x: x[2:])

    df['action_state'] = action_state
    df = df.loc[df.ActionGeo_ADM1Code != "US"]

    return df


def normalize_goldstein(df):

    print 'In normalize goldstein function'

    df = df.rename(columns={'CAST(GoldsteinScale AS FLOAT)': 'goldsteinscale',
                            'CAST(SQLDATE AS INTEGER)': 'sqldate'})

    df = df.loc[df.goldsteinscale > -20.0]

    min_scale, max_scale = -10.000005, 10.000005
    norm_gold = df['goldsteinscale'].apply(
        lambda x: (x - min_scale) / (max_scale - min_scale))

    df['norm_scale'] = norm_gold

    return df


def clean_df(df):

    df = df.drop(['Actor1Code',
                  'ActionGeo_FullName',
                  'ActionGeo_ADM1Code',
                  'Actor1Geo_CountryCode',
                  'sqldate',
                  'goldsteinscale'],
                 axis=1)

    return df


def aggregate_data(df):

    print 'In aggregate data function'

    df = df.groupby(['action_state', 'Year', 'MonthYear', 'Actor1Type1Code'],
                    as_index=False).agg({
                        "GLOBALEVENTID": ["count"],
                        "norm_scale": ["sum"]}).rename(columns={'GLOBALEVENTID': 'events_count',
                                                                'norm_scale': 'norm_scale_sum'})

    pattern = re.compile("^[a-zA-Z]+$")
    df = df.loc[df.action_state.str.contains(pattern)]

    df.columns = df.columns.droplevel(1)
    print list(df)

    return df


def batch_update(df, con):

    dict_df = df.to_dict(orient='records')

    cur = con.cursor()

    try:
        cur.executemany(
            '''
                UPDATE monthyr_central_results
                SET
                    events_count = events_count + %(events_count)s,
                    norm_scale = norm_scale + %(norm_scale_sum)s
                WHERE
                    action_state = %(action_state)s AND
                    month_year = %(MonthYear)s AND
                    actor_type = %(Actor1Type1Code)s
            ''',
            dict_df
        )

        cur.close()
        con.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print 'DB Error - ' + str(error)
    finally:
        if con is not None:
            con.close()

    return df


def get_db_conn():

    config = configparser.ConfigParser()
    # TODO: Make sure to read the correct config.ini file on AWS workers
    config.read('/home/ubuntu/Insight-GDELT-Feed/gdelt/config.ini')
    dbname = config.get('dbauth', 'dbname')
    dbuser = config.get('dbauth', 'user')
    dbpass = config.get('dbauth', 'password')
    dbhost = config.get('dbauth', 'host')
    dbport = config.get('dbauth', 'port')

    db = create_engine('postgres://%s%s/%s' % (dbuser, dbhost, dbname))
    con = None
    con = psycopg2.connect(
        database=dbname,
        host=dbhost,
        user=dbuser,
        password=dbpass)

    return con


if __name__ == '__main__':

    con = get_db_conn()

    data_gather = DataGatherer()
    data_gather.set_target_file()
    data_gather.download_zip()
    data_gather.unzip_download()
    df = data_gather.get_csv_dataframe()

    df = initial_df_clean(df)
    df = get_states(df)
    df = normalize_goldstein(df)
    df = clean_df(df)
    df = aggregate_data(df)
    df = batch_update(df, con)

    data_gather.delete_recent_files()

    print 'Done'

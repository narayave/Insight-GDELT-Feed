from six.moves import configparser
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pandasql as ps
from pprint import pprint
import re


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
                        sqldate >= 20190216 and
                        Actor1Type1Code in ('COP', 'GOV', 'JUD', 'BUS',
                                            'CRM', 'DEV', 'EDU', 'ENV',
                                            'HLH', 'LEG','MED','MNC');
        """

    try:
        dataframe = pd.read_sql_query(sql_query, con)
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
    norm_gold = df['goldsteinscale'].apply(
        lambda x: (x - min_scale) / (max_scale - min_scale))

    # print norm_gold

    df['norm_scale'] = norm_gold
    # pprint(df)

    return df


def clean_df(df):

    df = df.drop(['actor1code',
                  'actiongeo_fullname',
                  'actiongeo_adm1code',
                  'actor1geo_countrycode',
                  'sqldate',
                  'goldsteinscale'],
                 axis=1)

    # pprint(df)

    return df


def aggregate_data(df):

    print 'In aggregate data function'

    df = df.groupby(['action_state', 'year', 'monthyear', 'actor1type1code'],
                    as_index=False).agg({
                        "globaleventid": ["count"],
                        "norm_scale": ["sum"]}).rename(columns={'globaleventid': 'events_count',
                                                                'norm_scale': 'norm_scale_sum'})

    pattern = re.compile("^[a-zA-Z]+$")
    df = df.loc[df.action_state.str.contains(pattern)]

    pprint(df)

    df.columns = df.columns.droplevel(1)
    print list(df)

    return df


def get_table_data(df):

    print 'In get table data function'

    pprint(df)
    print list(df)

    unq_states = df.action_state.unique()
    unq_years = df.year.unique()
    unq_mnthyr = df.monthyear.unique()
    unq_type = df.actor1type1code.unique()

    print unq_states
    print unq_years
    print unq_mnthyr
    print unq_type

    return df


def batch_update(df, con):

    dict_df = df.to_dict(orient='records')
    # pprint(dict_df)

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
                    month_year = %(monthyear)s AND
                    actor_type = %(actor1type1code)s
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


if __name__ == '__main__':

    config = configparser.ConfigParser()
    # TODO: Make sure to read the correct config.ini file on AWS workers
    config.read('/home/vee/repos/Insight-GDELT-Feed/gdelt/config.ini')
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

    df = get_df(con)
    df = get_states(df)
    df = normalize_goldstein(df)
    df = clean_df(df)
    df = aggregate_data(df)
    # df = get_table_data(df)
    df = batch_update(df, con)

    print 'Done'

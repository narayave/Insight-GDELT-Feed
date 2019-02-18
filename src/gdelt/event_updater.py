import re

from sqlalchemy import create_engine
from six.moves import configparser
import pandas as pd
import pandasql as ps
from pprint import pprint
import psycopg2

from data_gatherer import DataGatherer


class EventUpdater(object):

    def __init__(self):
        pass

    def initial_df_clean(self,  dataframe):
        '''
            Given a pandas dataframe, this function the necessary column data
            and forgoes the rest.
        '''

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

    def get_states(self, df):
        '''
            This function accomplishes 2 different things at once.
                1. From the 'ActionGeo_ADM1Code' column, it parses the state
                    of the event.
                2. Events with state information are filtered out.
        '''

        action_state = df['ActionGeo_ADM1Code'].apply(lambda x: x[2:])

        df['action_state'] = action_state
        df = df.loc[df.ActionGeo_ADM1Code != "US"]

        return df

    def normalize_goldstein(self, df):
        '''
            The goldstein scale ranges between -10.0 and 10.0. Typical scores
            and up between -1.0 to 1.0. This function helps normalize the scale
            to fall between 0.0 and 1.0.

            Normalization equation is the following:
                new_score = (x - original_min) / (original_max - original_min)
        '''

        print 'In normalize goldstein function'

        df = df.rename(columns={
            'CAST(GoldsteinScale AS FLOAT)': 'goldsteinscale',
            'CAST(SQLDATE AS INTEGER)': 'sqldate'
        }
        )

        df = df.loc[df.goldsteinscale > -20.0]

        min_scale, max_scale = -10.000005, 10.000005
        norm_gold = df['goldsteinscale'].apply(
            lambda x: (x - min_scale) / (max_scale - min_scale))

        df['norm_scale'] = norm_gold

        return df

    def clean_df(self, df):
        '''
            Once necessary fields have been computed, certain columns can be
            ignored. This function helps accomplish that by cleaning up the
            columns.
        '''

        df = df.drop(['Actor1Code',
                      'ActionGeo_FullName',
                      'ActionGeo_ADM1Code',
                      'Actor1Geo_CountryCode',
                      'sqldate',
                      'goldsteinscale'],
                     axis=1)

        return df

    def aggregate_data(self, df):
        '''
            This function groups the data appropriately, and allows for quick
            summing and counting of paraticular columns.
        '''

        print 'In aggregate data function'

        df = df.groupby(['action_state', 'Year', 'MonthYear',
                         'Actor1Type1Code'],
                        as_index=False).agg({
                            "GLOBALEVENTID": ["count"],
                            "norm_scale": ["sum"]}).rename(columns={
                                'GLOBALEVENTID': 'events_count',
                                'norm_scale': 'norm_scale_sum'})

        pattern = re.compile("^[a-zA-Z]+$")
        df = df.loc[df.action_state.str.contains(pattern)]

        df.columns = df.columns.droplevel(1)
        print list(df)

        return df

    def batch_update(self, df, con):
        '''
            The pandas dataframe that is passed to this function is turned into
            a dictionary. The database connection is used to update the
            dataframe entries to the database.
        '''

        dict_df = df.to_dict(orient='records')

        pprint(dict_df)

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

    def get_db_conn(self):
        '''
            A connection to the database is established using this function.
        '''

        config = configparser.ConfigParser()
        # TODO: Make sure to read the correct config.ini file on AWS workers
        config.read('/home/ubuntu/Insight-GDELT-Feed/src/gdelt/config.ini')
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
    data_updater = EventUpdater()

    data_gather.set_target_file()
    data_gather.download_zip()
    data_gather.unzip_download()
    df = data_gather.get_csv_dataframe()

    df = data_updater.initial_df_clean(df)
    df = data_updater.get_states(df)
    df = data_updater.normalize_goldstein(df)
    df = data_updater.clean_df(df)
    df = data_updater.aggregate_data(df)
    df = data_updater.batch_update(df, con)

    data_gather.delete_recent_files()

    print 'Done'

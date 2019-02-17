import psycopg2
import urllib2
from six.moves import configparser
from smart_open import smart_open
from pprint import pprint

from urllib import urlopen
from zipfile import ZipFile
import urllib
import pandas as pd

class Database_Operations(object):

    def __init__(self, config):
        config.read('/home/ubuntu/Insight-GDELT-Feed/gdelt/config.ini')
        self.__db_name = config.get('dbauth', 'dbname')
        self.__db_user = config.get('dbauth', 'user')
        self.__db_pass = config.get('dbauth', 'password')
        self.__db_host = config.get('dbauth', 'host')
        self.__db_port = config.get('dbauth', 'port')

    """
        All commands should be sent to this function execute
    """
    def db_command(self, commands):

        conn = None
        try:
            # connect to postgresql server
            conn = psycopg2.connect("dbname=" + self.__db_name +
                                    " host=" + self.__db_host +
                                    " port= " + self.__db_port +
                                    " user=" + self.__db_user +
                                    " password=" + self.__db_pass)
            cur = conn.cursor()

            # to get results for each operation
            results = []

            # execute each command
            for comm in commands:
                cur.execute(comm)
            # results.append(cur.fetchall())
            cur.close()
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print 'DB Error - ' + str(error)
        finally:
            if conn is not None:
                conn.close()
                return results


class Data_Gatherer(object):

    def __init__(self):
        # Super important link, updated every 15 minutes
        self.target_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.target_file = None
        self.primary_fields = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear',
            'Year', 'FractionDate', 'Actor1Code', 'Actor1Name',
            'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode',
            'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code',
            'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name',
            'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode',
            'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code',
            'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode',
            'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale',
            'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
            'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
            'Actor1Geo_ADM1Code', 'gap_1', 'Actor1Geo_Lat', 'Actor1Geo_Long',
            'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName',
            'Actor2Geo_CountryCode', 'gap_2', 'Actor2Geo_ADM1Code',
            'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID',
            'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
            'ActionGeo_ADM1Code', 'gap_3', 'ActionGeo_Lat', 'ActionGeo_Long',
            'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']
        self.target_file_url = None

    def set_target_file(self):

        target_file_link = None

        data = urlopen(self.target_url)
        for line in data:
            target_file_link = line
            break

        # file size, hash, link to zip
        target_link = target_file_link.split(" ")[2]
        self.target_file_url = target_link

        target_file = target_link.split("/")[-1]
        target_filename = target_file.replace(".zip\n", "")
        print 'Target file - ' + target_file
        print 'Target file URL - ' + target_link

        self.target_file = target_filename

    def download_zip(self):
        print 'Going to download latest GDELT update file'
        # urllib.request.urlretrieve(self.target_file_url,
        # "/home/ubuntu/Insight-GDELT-Feed/gdelt/")
        urllib.urlretrieve(
            self.target_file_url,
            "/home/ubuntu/Insight-GDELT-Feed/gdelt/" +
            self.target_file +
            ".zip")

    def unzip_download(self):
        filename = '/home/ubuntu/Insight-GDELT-Feed/gdelt/' + self.target_file + '.zip'
        print 'To unzip - ' + filename

        with ZipFile(filename, 'r') as zip:
            # extracting all the files
            print('Extracting all the files now...')
            zip.extractall('/home/ubuntu/Insight-GDELT-Feed/gdelt/')
            print('Done!')


    def delete_recent_files(self):

        print 'Going to remove some files'
        os.remove('/home/ubuntu/Insight-GDELT-Feed/gdelt/' + self.target_file)
        os.remove('/home/ubuntu/Insight-GDELT-Feed/gdelt/' + self.target_file + '.zip')
        print 'Removed those recent files'


    def load_gdelt_csv(self, data_ops_handler, target=None):

        command = [ "COPY events FROM '/home/ubuntu/Insight-GDELT-Feed/gdelt/" + self.target_file + "' delimiter '\t' csv;" ]
        print command

        results = data_ops_handler.db_command(command)
        print results

    def get_csv_dataframe(self):

        dataframe = pd.read_csv("/home/ubuntu/Insight-GDELT-Feed/gdelt/" + self.target_file,
                        sep='\t', header = None)
        dataframe.columns = self.primary_fields

        return dataframe


if __name__ == '__main__':

    config = configparser.ConfigParser()
    db_ops = Database_Operations(config)
    data_gather = Data_Gatherer()

    data_gather.set_target_file()
    data_gather.download_zip()
    data_gather.unzip_download()

    data_gather.load_gdelt_csv(db_ops)
    data_gather.delete_recent_files()

    print 'Done'

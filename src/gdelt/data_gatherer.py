import os
from urllib import urlopen
from urllib import urlretrieve
from zipfile import ZipFile

import pandas as pd


class DataGatherer(object):

    def __init__(self):
        # Super important link, updated every 15 minutes
        self.target_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.target_file = None
        self.primary_fields = [
            'GLOBALEVENTID',
            'SQLDATE',
            'MonthYear',
            'Year',
            'FractionDate',
            'Actor1Code',
            'Actor1Name',
            'Actor1CountryCode',
            'Actor1KnownGroupCode',
            'Actor1EthnicCode',
            'Actor1Religion1Code',
            'Actor1Religion2Code',
            'Actor1Type1Code',
            'Actor1Type2Code',
            'Actor1Type3Code',
            'Actor2Code',
            'Actor2Name',
            'Actor2CountryCode',
            'Actor2KnownGroupCode',
            'Actor2EthnicCode',
            'Actor2Religion1Code',
            'Actor2Religion2Code',
            'Actor2Type1Code',
            'Actor2Type2Code',
            'Actor2Type3Code',
            'IsRootEvent',
            'EventCode',
            'EventBaseCode',
            'EventRootCode',
            'QuadClass',
            'GoldsteinScale',
            'NumMentions',
            'NumSources',
            'NumArticles',
            'AvgTone',
            'Actor1Geo_Type',
            'Actor1Geo_FullName',
            'Actor1Geo_CountryCode',
            'Actor1Geo_ADM1Code',
            'gap_1',
            'Actor1Geo_Lat',
            'Actor1Geo_Long',
            'Actor1Geo_FeatureID',
            'Actor2Geo_Type',
            'Actor2Geo_FullName',
            'Actor2Geo_CountryCode',
            'gap_2',
            'Actor2Geo_ADM1Code',
            'Actor2Geo_Lat',
            'Actor2Geo_Long',
            'Actor2Geo_FeatureID',
            'ActionGeo_Type',
            'ActionGeo_FullName',
            'ActionGeo_CountryCode',
            'ActionGeo_ADM1Code',
            'gap_3',
            'ActionGeo_Lat',
            'ActionGeo_Long',
            'ActionGeo_FeatureID',
            'DATEADDED',
            'SOURCEURL']
        self.target_file_url = None

    def set_target_file(self):
        '''
            This function reads the target_url to find target file. It sets
            the local variables for the target file, and the location for
            where it can be found.
        '''

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
        '''
            This function's primary purpose is to download the target file.
        '''

        print 'Going to download latest GDELT update file'
        urlretrieve(
            self.target_file_url,
            "/home/ubuntu/Insight-GDELT-Feed/src/gdelt/" +
            self.target_file +
            ".zip")

    def unzip_download(self):
        '''
            This function unzips the targets file that was previously
            downloaded.
        '''
        filename = '/home/ubuntu/Insight-GDELT-Feed/src/gdelt/' + \
            self.target_file + '.zip'
        print 'To unzip - ' + filename

        with ZipFile(filename, 'r') as zip:
            # extracting all the files
            print('Extracting all the files now...')
            zip.extractall('/home/ubuntu/Insight-GDELT-Feed/src/gdelt/')
            print('Done!')

    def get_csv_dataframe(self):
        '''
            Using pandas, the csv that was unzipped is loaded as a pandas
            dataframe. The columns are attached. The dataframe is then
            returned for use.
        '''

        dataframe = pd.read_csv(
            "/home/ubuntu/Insight-GDELT-Feed/src/gdelt/" +
            self.target_file,
            sep='\t',
            header=None,
            encoding='utf-8')
        dataframe.columns = self.primary_fields

        return dataframe

    def delete_recent_files(self):
        '''
            When called, this function helps delete the csv and the zipped
            file that was it was originally contained in.
        '''

        print 'Going to remove some files'
        os.remove('/home/ubuntu/Insight-GDELT-Feed/src/gdelt/' +
                  self.target_file)
        os.remove(
            '/home/ubuntu/Insight-GDELT-Feed/src/gdelt/' +
            self.target_file +
            '.zip')
        print 'Removed those recent files'

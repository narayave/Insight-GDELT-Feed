from smart_open import smart_open
import urllib2
import boto3
import botocore
import json


table_name = "twitter_stream_test"
dynamodb = boto3.client('dynamodb')


def get_target_filename():

    # Super important link, updated every 15 minutes
    target_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    target_file = ''
    data = urllib2.urlopen(target_url)
    for line in data:
        target_file = line
        break

    return target_file


def get_filename(location):
    print location

    # file size, hash, link to zip
    target_link = location.split(" ")[2]
    target_file = target_link.split("/")[-1]
    target_file = target_file.replace(".zip\n", "")
    print 'Target file - ' + target_file

    return target_file


def write_to_db(dict_line):
    
	print('Got data - ', created_at)	

	response = dynamodb.put_item(
		TableName=table_name,
		Item={
			'item_type': 'Event-'+ dict_line['GLOBALEVENTID'],
			'item_key': dict_line['GLOBALEVENTID'],
			'SQLDATE': dict_line['SQLDATE'],
            'ActionGeo_FullName': dict_line['ActionGeo_FullName'],
            'Actor1': {
                'Actor1Code': dict_line['Actor1Code'],
                'Actor1Name': dict_line['Actor1Name'],
                'Actor1Geo_FullName': dict_line['Actor1Geo_FullName'],
                },
            'Actor2': {
                'Actor2Code': dict_line['Actor2Code'],
                'Actor2Name': dict_line['Actor2Name'],
                'Actor2Geo_FullName': dict_line['Actor2Geo_FullName'],
                },
            'AvgTone': dict_line['AvgTone'],
            'DATEADDED': dict_line['DATEADDED'],
            'SOURCEURL': dict_line['SOURCEURL']
		}
	)


	print("PutItem succeeded:")
	print(json.dumps(response, indent=4, cls=DecimalEncoder))


def read_s3_contents(target_file):

    twitter_topics = []

    primary_fields = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', \
                    'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', \
                    'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', \
                    'Actor1Type1Code', 'Actor1Type2Code','Actor1Type3Code','Actor2Code', \
                    'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', \
                    'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', \
                    'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', \
                    'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', \
                    'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Filler1', 'Filler2', 'Filler3', \
                    'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', \
                    'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', \
                    'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', \
                    'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', \
                    'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', \
                    'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']

    for line in smart_open('s3://gdelt-open-data/v2/events/'+target_file, 'rb'):
        # print line.decode('utf8')
        if 'United States' not in line:
            # print line
            continue

        line = line.replace("\t", ";").split(';')
        # line = filter(None, line)
        # print line, type(line)
        print 'Size of line - ', len(line), 'Size of fields - ', len(primary_fields)

        # dict_line = {field: val for field, val in line if field in primary_fields}

        dict_line = {}
        for i in xrange(0, len(line)):
            dict_line[primary_fields[i]] = line[i]

        # print dict_line

        write_to_db(dict_line)

        # print line #[52]
        topic1 = line[6].lower()
        topic2 = line[52].split(",")[0]
        twitter_topics.append(topic1 + " " + topic2)


    print twitter_topics
    print 'Done'


if __name__ == '__main__':

    print 'Reporting for duty'

    target_file = get_target_filename()
    target_filename = get_filename(target_file)

    # read_s3_contents(target_filename.lower()) # TODO: FIX THIS
    read_s3_contents("20190124233000.export.csv")

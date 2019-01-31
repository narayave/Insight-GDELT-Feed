import psycopg2
import urllib2
from six.moves import configparser
from smart_open import smart_open
from pprint import pprint


def write_to_db(record, db_name, db_user, db_pass, db_host, db_port):

	# pprint(record)

	globaleventid = record['GLOBALEVENTID']
	sqldate = record['SQLDATE']
	actor1code = "'"+record['Actor1Code']+"'"
	actor1name = "'"+record['Actor1Name']+"'"
	actor1geo_fullname = "'"+record['Actor1Geo_FullName']+"'"
	actor1countrycode = "'"+record['Actor1Geo_CountryCode']+"'"
	actor2code = "'"+record['Actor2Code']+"'"
	actor2name = "'"+record['Actor2Name']+"'"
	actor2geo_fullname = "'"+record['Actor2Geo_FullName']+"'"
	actor2countrycode = "'"+record['Actor2Geo_CountryCode']+"'"
	goldstein_scale = record['GoldsteinScale']
	avg_tone = record['AvgTone']
	num_mentions = record['NumMentions']
	event_code = "'"+record['EventCode']+"'"
	source_url = "'"+record['SOURCEURL']+"'"

	example = "VALUES("+str(globaleventid)+","+str(sqldate)+","+actor1code+","+actor1name+","+ \
		actor1geo_fullname+","+actor1countrycode+","+actor2code+","+actor2name+","+ \
		actor2geo_fullname+","+actor2countrycode+","+str(goldstein_scale)+","+ \
		str(avg_tone)+","+ str(num_mentions)+","+ str(event_code)+","+ str(source_url)
	# pprint(example)

        command = [
            "INSERT INTO gdelt_events( \
                globaleventid, sqldate, actor1code, actor1name, actor1geo_fullname, \
                actor1countrycode, actor2code, actor2name, actor2geo_fullname, \
                actor2countrycode, goldstein_scale, avg_tone, num_mentions, event_code, \
                source_url) \
            VALUES("+str(globaleventid)+","+str(sqldate)+","+actor1code+","+actor1name+","+ \
            actor1geo_fullname+","+actor1countrycode+","+actor2code+","+actor2name+","+ \
            actor2geo_fullname+","+actor2countrycode+","+str(goldstein_scale)+","+ \
            str(avg_tone)+","+str(num_mentions)+","+str(event_code)+","+source_url+");"
        ]

	conn = None
	try:
		# connect to postgresql server
		conn = psycopg2.connect("dbname="+db_name+" host="+db_host+" port= "+db_port+" user="+db_user+" password="+db_pass)
		cur = conn.cursor()

		# create table
		for comm in command:
			cur.execute(comm)
		cur.close()
		conn.commit()
	except(Exception, psycopg2.DatabaseError) as error:
		print error
	finally:
		if conn is not None:
			conn.close()



def create_gdelt_table(db_name, db_user, db_pass, db_host, db_port):

	command = [
	"""CREATE TABLE gdelt_events (
		globaleventid INTEGER PRIMARY KEY,
		sqldate INTEGER,
		actor1code VARCHAR(10),
		actor1name VARCHAR(50),
		actor1geo_fullname VARCHAR(100),
		actor1countrycode VARCHAR(10),
		actor2code VARCHAR(10),
		actor2name VARCHAR(50),
		actor2geo_fullname VARCHAR(100),
		actor2countrycode VARCHAR(10),
		goldstein_scale NUMERIC,
		avg_tone NUMERIC,
		num_mentions INTEGER,
		event_code VARCHAR(10),
		source_url VARCHAR(200)
	);"""]

	conn = None
	try:
		# connect to postgresql server
		conn = psycopg2.connect("dbname="+db_name+" host="+db_host+" port= "+db_port+" user="+db_user+" password="+db_pass)
		cur = conn.cursor()

		# create table
		for comm in command:
			cur.execute(comm)
		cur.close()
		conn.commit()
	except(Exception, psycopg2.DatabaseError) as error:
		print error
	finally:
		if conn is not None:
			conn.close()

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


def read_s3_contents(target_file, db_name, db_user, db_pass, db_host, db_port):

	primary_fields = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', \
                    'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', \
                    'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', \
                    'Actor1Type1Code', 'Actor1Type2Code','Actor1Type3Code','Actor2Code', \
                    'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', \
                    'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', \
                    'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', \
                    'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', \
                    'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', \
                    'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'gap_1', \
                    'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', \
                    'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'gap_2', 'Actor2Geo_ADM1Code', \
                    'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', \
                    'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'gap_3', \
                    'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']

	for line in smart_open('s3://gdelt-open-data/v2/events/'+target_file, 'rb'):

		line = line.split('\t')
		print 'Size of line - ', len(line), 'Size of fields - ', len(primary_fields)

		dict_line = dict(zip(primary_fields, line))
		write_to_db(dict_line, db_name, db_user, db_pass, db_host, db_port)
		break

	print 'Read and dumped events to db'


def psqltest_query(tone_select, db_name, db_user, db_pass, db_host, db_port):

	command = "SELECT * FROM gdelt_events WHERE gdelt_events.avg_tone < " + str(tone_select) + ";"

	conn = None
	try:
		# connect to postgresql server
		conn = psycopg2.connect("dbname="+db_name+" host="+db_host+" port= "+db_port+" user="+db_user+" password="+db_pass)
		cur = conn.cursor()

		cur.execute(command)
		data = cur.fetchall()
		print data
		cur.close()
		conn.commit()
		print 'I did something....?'
	except(Exception, psycopg2.DatabaseError) as error:
		print error
	finally:
		if conn is not None:
			conn.close()


if __name__ == '__main__':

	config = configparser.ConfigParser()
	config.read('config.ini')
	db_name = config.get('dbauth', 'dbname')
	db_user = config.get('dbauth', 'user')
	db_pass = config.get('dbauth', 'password')
	db_host = config.get('dbauth', 'host')
	db_port = config.get('dbauth', 'port')

	# target_file = get_target_filename()
	# target_filename = get_filename(target_file)

	create_gdelt_table(db_name, db_user, db_pass, db_host, db_port)

	#read_s3_contents(target_filename.lower()) # TODO: FIX THIS
	read_s3_contents("20190124233000.export.csv", db_name, db_user, db_pass, db_host, db_port)

	tone_select = -4.0
	psqltest_query(tone_select, db_name, db_user, db_pass, db_host, db_port)

	print 'Done'




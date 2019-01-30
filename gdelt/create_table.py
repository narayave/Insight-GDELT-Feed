import psycopg2
from six.moves import configparser

def create_gdelt_table(db_name, db_user, db_pass):

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
	)"""]

	conn = None
	try:
		# connect to postgresql server
		conn = psycopg2.connect("dbname="+db_name+" user="+db_user+" password="+db_pass)
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


if __name__ == '__main__':

	config = configparser.ConfigParser()
	config.read('config.ini')
	db_name = config.get('dbauth', 'dbname')
	db_user = config.get('dbauth', 'user')
	db_pass = config.get('dbauth', 'password')

	create_gdelt_table(db_name, db_user, db_pass)

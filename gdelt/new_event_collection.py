import psycopg2
import urllib2
from six.moves import configparser
from smart_open import smart_open
from pprint import pprint

class Database_Operations(object):

    def __init__(self, config):
        config.read('config.ini')
        self.__db_name = config.get('dbauth', 'dbname')
        self.__db_user = config.get('dbauth', 'user')
        self.__db_pass = config.get('dbauth', 'password')
        self.__db_host = config.get('dbauth', 'host')
        self.__db_port = config.get('dbauth', 'port')


    """
        All commands should be sent to this function execute
    """
    def db_command(self, command):

        conn = None
        try:
            # connect to postgresql server
            conn = psycopg2.connect("dbname=" + self.__db_name+ \
                                    " host=" + self.__db_host+ \
                                    " port= " + self.__db_port+ \
                                    " user=" + self.__db_user+ \
                                    " password=" + self.__db_pass)
            cur = conn.cursor()

            # to get results for each operation
            results = []

            # execute each command
            for comm in command:
                cur.execute(comm)
                results.append(cur.fetchall())
            cur.close()
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print 'DB Error - ' + str(error)
        finally:
            if conn is not None:
                conn.close()
                return results


    def create_gdelt_table(self):

        command = [
        """CREATE TABLE gdelt_events (
            globaleventid INTEGER PRIMARY KEY, sqldate INTEGER,
            actor1code VARCHAR(10), actor1name VARCHAR(50),
            actor1geo_fullname VARCHAR(100), actor1countrycode VARCHAR(10),
            actor2code VARCHAR(10), actor2name VARCHAR(50),
            actor2geo_fullname VARCHAR(100), actor2countrycode VARCHAR(10),
            goldstein_scale NUMERIC, avg_tone NUMERIC, num_mentions INTEGER,
            event_code VARCHAR(10), source_url VARCHAR(200)
        );"""]

        results = self.db_command(command)
        pprint(results)


class Data_Gatherer(object):

    def __init__(self):
        self.target_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.target_file = None


    def set_target_file(self):

        # Super important link, updated every 15 minutes
        target_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        target_file_link = None

        data = urllib2.urlopen(target_url)
        for line in data:
            target_file_link = line
            break

        # file size, hash, link to zip
        target_link = target_file_link.split(" ")[2]
        target_file = target_link.split("/")[-1]
        target_filename = target_file.replace(".zip\n", "")
        print 'Target file - ' + target_file

        self.target_file = target_filename


def example_psql_query(db_ops, ton_select):
    
    command = ["SELECT * FROM gdelt_events WHERE gdelt_events.avg_tone < " + \
                str(tone_select) + "ORDER BY gdelt_events.sqldate DESC;"]

    results = db_ops.db_command(command)
    pprint(results)



if __name__ == '__main__':

    config = configparser.ConfigParser()
    db_ops = Database_Operations(config)
    data_gather = Data_Gatherer()

    data_gather.set_target_file()

    db_ops.create_gdelt_table()

    # tone_select = -4.0
    # example_psql_query(db_ops, tone_select)

    print 'Done'
from six.moves import configparser


class DatabaseOperations(object):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('/home/ubuntu/test-Insight/src/spark/config.ini')
        self.__db_name = config.get('dbauth', 'dbname')
        self.__db_user = config.get('dbauth', 'user')
        self.__db_pass = config.get('dbauth', 'password')
        self.__db_host = config.get('dbauth', 'host')
        self.__db_port = config.get('dbauth', 'port')

        self.__table_name = None

        self.__write_mode = 'append'
        self.__url = "jdbc:postgresql://" + \
            self.__db_host + \
            ":" + \
            self.__db_port + \
            "/" + \
            self.__db_name

        self.__properties = {
            "driver": "org.postgresql.Driver",
            "user": self.__db_user,
            "password": self.__db_pass
        }

    def set_table_name(self, table_name):
        '''
            Set table name
        '''

        self.__table_name = table_name

    def db_write(self, dataframe):
        '''
            This function takes in a dataframe and appends it to the table
        '''

        dataframe.write \
        .jdbc(url=self.__url,
              table=self.__table_name,
              mode=self.__write_mode,
              properties=self.__properties)

        print 'Done writing'

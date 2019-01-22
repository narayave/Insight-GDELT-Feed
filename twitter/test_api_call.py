#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from six.moves import configparser #ConfigParser

#Variables that contains the user credentials to access Twitter API 
#config = configparser.ConfigParser()
#config.read('config.ini')
#access_token = config.get('auth', 'api_access_token')
#access_token_secret = config.get('auth', 'api_access_token_secret')
#consumer_key = config.get('auth', 'api_consumer_key')
#consumer_secret = config.get('auth', 'api_consumer_secret')

access_token = "ZJ1C1T3dpZGYwTBtLoLtqljHZ"
access_token_secret = "ECU0biftiAfksMB1Q11KcjXqFU5DzxXuzHMrJVyelBzpZpMlAu"
consumer_key = "1086371836420653058-oZLpiEbnHa2uTLtCh93GmxpmRxIvpq"
consumer_secret = "gyRYc4VZI1PTWlj2c9fLjFVtbFD4M1SIpeIeVoNxNiys3"

print access_token
print access_token_secret
print consumer_key
print consumer_secret

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print 'Status - ', status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['trump', 'mueller', 'fbi'])

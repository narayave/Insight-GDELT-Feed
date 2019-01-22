#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import config
from kafka import SimpleProducer, KafkaClient
from six.moves import configparser

#Variables that contains the user credentials to access Twitter API
config = configparser.ConfigParser()
config.read('config.ini')
access_token = config.get('auth', 'access_token')
access_token_secret = config.get('auth', 'access_token_secret')
consumer_key = config.get('auth', 'consumer_key')
consumer_secret = config.get('auth', 'consumer_secret')

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("tweetdata", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print ("Status - ", status)


#This is a basic listener that just prints received tweets to stdout.


if __name__ == '__main__':

    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['trump', 'mueller', 'fbi'])

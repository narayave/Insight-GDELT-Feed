#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import config
from kafka import SimpleProducer, KafkaClient

#Variables that contains the user credentials to access Twitter API 
access_token = config.API_ACCESS_TOKEN
access_token_secret = config.API_ACCESS_TOKEN_SECRET
consumer_key = config.API_CONSUMER_KEY
consumer_secret = config.API_CONSUMER_SECRET

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

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
from six.moves import configparser
from multiprocessing.pool import ThreadPool


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



def start_streaming(stream):

    def map_func(topics):
        stream.filter(track=topics)


    print 'I can start streaming'
    #topic_list = [["residents Maplewood"], ["united states maplewood"]]
    topic_list= ['residents Maplewood', 'united states Maplewood','united states Maine','nasa United States','nasa United States','united states Michigan','senate Frankfurt',' Richmond',' Pulaski County',' Portland',' Minnesota',' Wisconsin','criminal Maury County','governor Marinette']

    #pool = ThreadPool()
    #pool.map(map_func, topic_list)
    stream.filter(track=topic_list)


if __name__ == '__main__':

    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    start_streaming(stream)

    print 'Done'


    #This line filter Twitter Streams to capture data by the keywords:
    # stream.filter(track=['trump', 'mueller', 'fbi'])

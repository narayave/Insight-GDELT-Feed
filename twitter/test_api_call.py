#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "1086371836420653058-oZLpiEbnHa2uTLtCh93GmxpmRxIvpq"
access_token_secret = "gyRYc4VZI1PTWlj2c9fLjFVtbFD4M1SIpeIeVoNxNiys3"
consumer_key = "ZJ1C1T3dpZGYwTBtLoLtqljHZ"
consumer_secret = "ECU0biftiAfksMB1Q11KcjXqFU5DzxXuzHMrJVyelBzpZpMlAu"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['trump', 'mueller', 'fbi'])
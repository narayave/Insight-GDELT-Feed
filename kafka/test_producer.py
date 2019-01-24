from kafka import SimpleProducer, KafkaClient
from time import sleep
import json

if __name__ == '__main__':

	print 'Start'
	kafka = KafkaClient("localhost:9092")

	print 'Got kafka client'
	producer = SimpleProducer(kafka)
	print 'Got producer'

	for i in xrange(0, 10):
		
		tweet = {}
		tweet["created_at"] = "Sat Jan 19 20:02:51 +0000 2019"
		tweet["data"] = "Object " + str(i)

		print 'Tweet to send - ', str(tweet)
		tweet_json = json.dumps(tweet)
		producer.send_messages("tweetdata", tweet_json.encode('utf-8'))
		print 'Sent message'
		sleep(5)	



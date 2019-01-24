from __future__ import print_function
from kafka import KafkaConsumer
import boto3
import json


#dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
table_name = "twitter_stream_test"

dynamodb = boto3.client('dynamodb')
#table = dynamodb.Table(table_name)

def put_item_in_db(data):
    
	item_type = 1
	created_at = data['created_at']

	print('Got data - ', created_at)	

	response = dynamodb.put_item(
		TableName=table_name,
		Item={
			'item_type': item_type,
			'created_at': created_at,
			'tweet': json.dumps(data)
		}
	)


	print("PutItem succeeded:")
	print(json.dumps(response, indent=4, cls=DecimalEncoder))


def consumer():

	print('In consumer method')
	# consume json messages
	consumer = KafkaConsumer("tweetdata", 
                        bootstrap_servers=['localhost:9092'],
	                value_deserializer=lambda m: json.loads(m.decode('ascii')))
	print('I have a consumer client')

	for mess in consumer:
		message = mess.value

        print(message)

        # Add to DynamoDB table
        # put_item_in_db(message)


if __name__ == '__main__':
	print('Going to call consumer method')
	consumer()


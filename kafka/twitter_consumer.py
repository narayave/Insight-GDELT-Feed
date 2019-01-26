from __future__ import print_function
from kafka import KafkaConsumer
import boto3
import json
import decimal


#dynamodb = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
table = dynamodb.Table("test_twitter_stream")

def put_item_in_db(data):

    item = 1
#    print(type(data))
#    print(data)
    created_at = json.loads(data)['created_at']

    print('Got data - ', created_at)

    response = table.put_item(
                Item={
                        'item_type': item,
                        'created_at': created_at,
                        'tweet': data
                }
        )


    print("PutItem succeeded:")
    print(json.dumps(response, indent=4)) #, cls=DecimalEncoder))


def consumer():

    print('In consumer method')
    # consume json messages
    consumer = KafkaConsumer("tweetdata", 
                        bootstrap_servers=['localhost:9092'],
                        value_deserializer=lambda m: m)
    print('I have a consumer client')

    for message in consumer:
        mess = message.value

        # print(mess)

        # Add to DynamoDB table
        put_item_in_db(mess)



if __name__ == '__main__':
    print('Going to call consumer method')
    consumer()

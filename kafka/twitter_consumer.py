from __future__ import print_function
from kafka import KafkaConsumer
import boto3
import json
import decimal


dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
table_name = "tweet-test"

table = dynamodb.Table(table_name)

def put_item_in_db(data):
    table.put_item(data)

    response = table.put_item(
    Item={
            'year': year,
            'title': title,
            'info': {
                'plot':"Nothing happens at all.",
                'rating': decimal.Decimal(0)
            }
        }
    )

    print("PutItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))

def consumer():

    # consume json messages
    consumer = KafkaConsumer("tweetdata", 
                        bootstrap_servers=['localhost:9092'],
                        value_deserializer=lambda m: json.loads(m.decode('ascii')))

    for message in consumer:
        mess = message.value

        print(mess)

        # Add to DynamoDB table
        # event_num = 1

        # print message


if __name__ == '__main__':
    consumer()
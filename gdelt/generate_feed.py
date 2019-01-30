from smart_open import smart_open
import urllib2
import boto3
#import botocore
import json
from boto3.dynamodb.conditions import Key, Attr
from pprint import pprint

table_name = "gdelt-news-feed"
dynamodb = boto3.resource('dynamodb')


def write_to_db(dict_line):
    
	print 'Dictionary line - ', str(dict_line)

	tmp_loc = dict_line['ActionGeo_Type'].split(', ')
	
	action_loc = tmp_loc[1] if len(tmp_loc) > 1 else tmp_loc[0]
	#action_loc = ','.join(tmp_loc[:2])
	print 'Action location state - ', action_loc

	table = dynamodb.Table(table_name)
	item = {
		'item': 'Event-'+ action_loc,
		'item_key': str(dict_line['GLOBALEVENTID']),
        'date': dict_line['SQLDATE'],
        'Location': action_loc,
		'SOURCEURL': dict_line['SOURCEURL'],
		'AvgTone': dict_line['AvgTone'],
		'DATEADDED': dict_line['DATEADDED']

		}	
	print item

	# response = dynamodb.put_item(TableName=table_name, Item=item)
	response = table.put_item(Item=item)

	print("PutItem succeeded:")
	print(json.dumps(response, indent=4)) #, cls=DecimalEncoder))


def write_interests():

    items_to_store = []

    table = dynamodb.Table(table_name)
    
    item1 = {
        'item': 'userInt-Massachusetts',
        'item_key': 'user1'
    }
    items_to_store.append(item1)
    item4 = {
        'item': 'userInt-Oregon',
        'item_key': 'user1'
    }
    items_to_store.append(item4)
    item2 = {
        'item': 'userInt-Massachusetts',
        'item_key': 'user2'
    }
    items_to_store.append(item2)
    item5 = {
        'item': 'userInt-New York',
        'item_key': 'user2'
    }
    items_to_store.append(item5)
    item3 = {
        'item': 'userInt-Massachusetts',
        'item_key': 'user3'
    }
    items_to_store.append(item3)
    item6 = {
        'item': 'userInt-Oregon',
        'item_key': 'user3'
    }
    items_to_store.append(item6)
    item7 = {
        'item': 'userInt-Oregon',
        'item_key': 'user4'
    }
    items_to_store.append(item7)
    item8 = {
        'item': 'userInt-Washington',
        'item_key': 'user5'
    }
    items_to_store.append(item8)

    with table.batch_writer() as batch:
        for i in items_to_store:
            batch.put_item(Item=i)

    print 'Done'


def add_new_users(start, end):

    items_to_store = []

    table = dynamodb.Table(table_name)

    for i in xrange(start, end+1):
        username = 'user'+str(i)
        item = {
        # items_to_store[i] = {
        'item': 'user-'+username,
        'item_key': username,
        'password': 'password',
        'name': 'User '+ str(i)
        }
        items_to_store.append(item)

    # print items_to_store

    with table.batch_writer() as batch:
        for i in items_to_store:
            batch.put_item(Item=i)

    print 'Done adding new users'


def generate_feed(username):
    table = dynamodb.Table(table_name)

    fe = Key('item').begins_with("Event-")
    response = table.scan(FilterExpression=fe)
    pprint(response)



def get_events(key):
	table = dynamodb.Table(table_name)

	fe = Key('item').eq(key)
	response = table.scan(FilterExpression=fe)

	#print response

	for item in response['Items']:
		print item





if __name__ == '__main__':

    print 'Going to generate feeds for users'

    # get_events('user-user1')
    # write_interests()
    # add_new_users(11,15)

    generate_feed('user1')
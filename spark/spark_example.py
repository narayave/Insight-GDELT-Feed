import argparse
from pyspark import SparkContext, SparkConf
# from boto.s3.connection import S3Connection
import boto
import botocore

def main():
    # Use argparse to handle some argument parsing

    # Use Boto to connect to S3 and get a list of objects from a bucket
    # conn = S3Connection(args.aws_access_key_id, args.aws_secret_access_key)
    # conn = conn.get_bucket(args.bucket_name)

    s3 = boto3.resource('s3', region_name='us-east-1')

    bucket = conn.get_bucket("s3n://gdelt-open-data/vs/events/")

    i = 0
    for object in bucket.objects.all():
        print(object)

        i += 1
        if i > 10:
            break

    print 'Done'




    #keys = bucket.objects.all()

    #print 'Got this far'


    # Get a Spark context and use it to parallelize the keys
    #conf = SparkConf().setAppName("GDELT-news")
    #sc = SparkContext(conf=conf)
    #pkeys = sc.parallelize(keys)

    # Call the map step to handle reading in the file contents
    #activation = pkeys.flatMap(map_func)
    # Additional map or reduce steps go here...

def map_func(key):
    # Use the key to read in the file contents, split on line endings
    for line in key.get_contents_as_string().splitlines():

        print line

        # # parse one line of json
        # j = json.loads(line)
        # if "user_id" in j && "event" in j:
        #     if j['event'] == "event_we_care_about":
        #         yield j['user_id'], j['event']

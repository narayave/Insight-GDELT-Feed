import argparse
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection

def main():
    # Use argparse to handle some argument parsing
    # parser.add_argument("-a",
    #                     "--aws_access_key_id",
    #                     help="AWS_ACCESS_KEY_ID, omit to use env settings",
    #                     default=None)
    # parser.add_argument("-s",
    #                     "--aws_secret_access_key",
    #                     help="AWS_SECRET_ACCESS_KEY, omit to use env settings",
    #                     default=None)
    # parser.add_argument("-b",
    #                     "--bucket_name",
    #                     help="AWS bucket name",
    #                     default="gdelt-open-data")
    # Use Boto to connect to S3 and get a list of objects from a bucket
    # conn = S3Connection(args.aws_access_key_id, args.aws_secret_access_key)
    conn = conn.get_bucket(args.bucket_name)
    bucket = conn.get_bucket("s3://gdelt-open-data/vs/events/")
    keys = bucket.list()
    # Get a Spark context and use it to parallelize the keys
    conf = SparkConf().setAppName("GDELT-news")
    sc = SparkContext(conf=conf)
    pkeys = sc.parallelize(keys)
    # Call the map step to handle reading in the file contents
    activation = pkeys.flatMap(map_func)
    # Additional map or reduce steps go here...

def map_func(key):
    # Use the key to read in the file contents, split on line endings
    for line in key.get_contents_as_string().splitlines():
        # parse one line of json
        j = json.loads(line)
        if "user_id" in j && "event" in j:
            if j['event'] == "event_we_care_about":
                yield j['user_id'], j['event']
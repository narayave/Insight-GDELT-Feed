import boto3
import botocore
import os

# Let's use Amazon S3
s3 = boto3.resource('s3', region_name='us-east-1')

bucket_name = 'gdelt-open-data'
bucket = s3.Bucket(bucket_name)

origin_filename = 'events/20190122.export.csv'
destination_filename = origin_filename.split('/')[1]
destination_dir = 'events/' # make sure this has the slash at the end

try:
        os.makedirs(os.path.dirname(destination_dir))
except:
        print 'Directory exists already'
        
bucket.download_file(origin_filename, destination_dir + destination_filename)


print 'Done'
import boto3
import botocore
import os

# Let's use Amazon S3
s3 = boto3.resource('s3', region_name='us-east-1')

bucket_name = 'gdelt-open-data'

bucket = s3.Bucket(bucket_name)

i = 0
for object in bucket.objects.all():
    print(object)

    i += 1
    if i > 10:
        break

print 'Done'
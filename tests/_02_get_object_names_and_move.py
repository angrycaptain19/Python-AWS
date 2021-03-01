import awswrangler as wr
# import pandas as pd
import boto3
# import os
# import pytz
# import json

aws_access_key_id = 'AKIAJGY2JXBNFOIZLJMQ'
aws_secret_access_key = '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'

session = boto3.Session(aws_access_key_id, aws_secret_access_key)
s3 = session.resource('s3')

# parameters for copy
bucketName = 'firstestdemo'
source_folder = 'source/'
target_folder = 'target_copy/'

# populate the bucket with elements
bucket = s3.Bucket(bucketName)
bucket_list = []

# get desired object names from bucket
# put it into a list
for obj in bucket.objects.all():
    if source_folder in obj.key:
        element = obj.key
        bucket_list.append(element.replace(source_folder, ''))

# print(bucket_list)

# copy selected objects to desired target folder
# delete the originals to simulate move effect
# there is no 'move' in S3
for filename in bucket_list:
    if len(filename) > 0:
        # print(filename)

        # copy objects from source to destination
        key_source = source_folder+filename
        key_to = target_folder+filename
        # print(key_source)
        # print(key_to)
        copy_source = {
            'Bucket': bucketName,
            'Key': key_source
        }
        s3.meta.client.copy(copy_source, bucketName, key_to)

        # Delete source objects - it works - commented!!
        # s3.Object(bucketName, key_source).delete()


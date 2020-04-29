# python  program to upload file to s3
import boto3


s3 = boto3.resource('s3')

MYBUCKET="cmurillo5raw"

for bucket in s3.buckets.all():
    print(bucket.name)

s3_client = boto3.client('s3')

s3_client.upload_file('./TDesCol.xlsx',MYBUCKET,'local/Covid19/Economicvars/TDesCol.xlsx')
s3_client.upload_file('./TesCol.xlsx',MYBUCKET,'local/Covid19/Economicvars/TesCol.xlsx')

print('finished...')
import json
import boto3
import urllib3
import os
import requests



def lambda_handler(event, context):
    # get the url
    url = event['Records'][0]['body'].split('"')[-2]

    bucket = 'iata-sales-record-archive'
    zipfile = url.split('/')[-1]
    key = zipfile

    # try to stream the URL directly into S3
    s3=boto3.client('s3')
    http=urllib3.PoolManager()
    # NOTE: I try to stream the URL directly to S3, however it gives an empty file
    #s3.upload_fileobj(http.request('GET', url, preload_content=False), bucket, key)
    
    # As an alternative, write to local filesystem (/tmp) using request
    req = requests.get(url)
    file = '/tmp/req' + zipfile
    with open(file,'wb') as output_file:
        output_file.write(req.content)
        print('Downloading Completed')

    s3.put_object(Bucket=bucket,Key=file)
    
    return {
        'statusCode': 200,
        'body': json.dumps('zip file downloaded')
    }

    
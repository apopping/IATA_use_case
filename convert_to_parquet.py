import json
import boto3
import os
import pandas as pd
import awswrangler as wr
from zipfile import ZipFile

def lambda_handler(event, context):
    # import .zip data from S3 bucket
    inbucket = 'iata-sales-record-archive'
    outbucket = 'iata-parquet-data'

    s3 = boto3.client("s3")

    for obj in s3.list_objects_v2(Bucket=inbucket)['Contents']:
        # look for .zip files in the root directory
        file = obj['Key'].split('/')[0]
        print(file)
        if file[-4:] == '.zip':
            # a zip file is recognised
            # extract the file and write to parquet
            infile = obj['Key']
            outfile = '/tmp/temp.zip'
            print(infile)
            print('getgoing')
            s3.download_file(inbucket, infile, outfile)
    
            with ZipFile(outfile, 'r') as zip:
                zip.extractall('/tmp/')
    
            files = os.listdir('/tmp/')
            # rename .csv file if it contains spaces
            for f in files:
                if f[-4:] == '.csv':
                    if ' ' in f:
                        new_name = str.replace(f, ' ','_')
                        print(f)
                        print(new_name)
                        os.rename('/tmp/' + f, '/tmp/' + new_name)
                

            print('rename finished')
            #s3.put_object(Bucket=outbucket,Key='/tmp/2m_sales_records.csv')
    
    
            # write the file to s3 
            s3 = boto3.resource('s3')
            s3.Bucket(outbucket).upload_file('/tmp/' + new_name, new_name)
    
    
    ##############################
    # convert the csv file to parquet
    
    files = os.listdir('/tmp/')
    for f in files:
        # check for csv files
        if f[-4:] == '.csv':
            data = pd.read_csv('/tmp/' + f)
       
            #df.to_parquet('output.parquet')
            output_file = 's3://iata-parquet-data/' + file[0:-4] + '.parquet'  

            wr.s3.to_parquet(
            df=data,
            path=output_file,
            dataset=True,
            partition_cols=['Country']
            )
  

    return {
        'statusCode': 200,
        'body': json.dumps('data has been converted to parquet')
    }

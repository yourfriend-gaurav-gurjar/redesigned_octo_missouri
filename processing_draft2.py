import sys
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import s3fs
import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
import logging
import os
import glob
import shutil
import s3fs

# dest_bucket = 'cannaspyglass-datalake-processed-dev'
# s3 = boto3.resource('s3')
s3client = boto3.client('s3', region_name='us-east-1')

# Download data from S3 Bucket
# source_bucket = 'cannaspyglass-datalake-raw'
# directory = "/home/ubuntu/Development/US/MO/LicenseInfor/Processing/raw_data/"
# access_rights = 0o755
# os.mkdir(directory, access_rights)
# Download data from S3 Bucket
# def download_data(s_bucket, s_key, filename):
#     s3client.download_file(s_bucket, s_key, filename)
#     print(filename)

# Processing data from S3 Bucket
source_bucket = 'cannaspyglass-datalake-raw'
dest_bucket = 'cannaspyglass-datalake-processed-dev'


# fs = s3fs.S3FileSystem(anon=True)
# fs.ls(source_bucket)

def p_data(s_bucket, file_path):
    raw_df = pd.read_excel(f's3://{s_bucket}/{file_path}/', header=1, skipfooter=0)
    raw_df.columns = ['status', 'licenseNumber','entityName','city','state','postalCode','firstName', 'lastName', 'phone']
    raw_df[['status']] = raw_df[['status']].fillna('No')
    raw_df = raw_df.fillna('NA')
    raw_df[['status']] = raw_df[['status']].replace('ü', 'Yes')
    print(raw_df)

# def p_data(s_bucket, file_path, d_bucket, d_file):
#     s3 = s3fs.S3FileSystem(anon=False)
#     raw_df = pd.read_excel(f's3://{s_bucket}/{file_path}/')
#     print(raw_df)
#     with s3.open(f's3://{d_bucket}/{d_file}/','w') as f:
#         raw_df.to_csv(f)


date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
filenames = ['cultivation_facility', 'dispensary_facility', 'infused_product_manufacturing_facility',
             'laboratory_testing_facility', 'transportation_facility']
for file in filenames:
    # source_bucket = source_bucket
    source_filename = 'US/MO/CannabisLifecycle/' + file + '_' + date + '.xlsx'
    # dest_filename = '/home/ubuntu/Development/US/MO/LicenseInfor/Processing/raw_data/' + file + '_' + date + '.csv'
    # raw_file = 'US/MO/CannabisLifecycle/LicenseInfo/' + file+'_' + date + '.xlsx'
    # print(dest_filename)
    # print(source_filename)
    # download_data(source_bucket, source_filename, dest_filename)
    p_data(source_bucket, source_filename)
    # p_data(source_bucket, source_filename, dest_bucket, dest_filename)

xlsxfiles = []

for file in glob.glob("*.csv"):
    xlsxfiles.append(file)

print(xlsxfiles)

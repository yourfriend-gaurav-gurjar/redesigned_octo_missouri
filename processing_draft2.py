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
# s3 = boto3.resource('s3')
s3client = boto3.client('s3', region_name = 'us-east-2')

source_bucket = 'cannaspyglass-datalake-raw-qa'
dest_bucket = 'cannaspyglass-datalake-processed-dev'

directory = "/home/ubuntu/Development/US/MO/LicenseInfor/Processing/raw_data"
access_rights=0o755
os.mkdir(directory, access_rights)
# Download data from S3 Bucket 
def download_data(source_bucket, source_key, filename):
    s3client.download_file(source_bucket, source_key, filename)
    # s3.download_file(source_bucket, source_key, filename)

date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
print(date)
filenames = ['cultivation_facility','dispensary_facility','infused_product_manufacturing_facility','laboratory_testing_facility','transportation_facility']
for file in filenames:
    source_filename = '/US/MO/CannabisLifecycle/LicenseInfo/' + file+'_' + date + '.xlsx'
    dest_filename = '/home/ubuntu/Development/US/MO/LicenseInfor/Processing/raw_data/' + file + '_'  + date + '.xlsx'
    # raw_file = 'US/MO/CannabisLifecycle/LicenseInfo/' + file+'_' + date + '.xlsx'
    print(dest_filename)
    print(source_filename)
    print(file)
    download_data(source_bucket, source_filename, dest_filename)

xlsxfiles = []
for file in glob.glob("*.xlsx"):
    xlsxfiles.append(file)

print(xlsxfiles)
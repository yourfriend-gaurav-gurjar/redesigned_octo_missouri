import pandas as pd
from datetime import datetime, timedelta
import boto3
import s3fs

s3client = boto3.client('s3', region_name='us-east-1')

# Processing data from S3 Bucket
source_bucket = 'cannaspyglass-datalake-raw'
dest_bucket = 'cannaspyglass-datalake-processed-dev'

# Processing the data from Raw format to processed
def p_data(s_bucket, file_path, d_bucket, d_file):
    s3 = s3fs.S3FileSystem(anon=False)
    raw_df = pd.read_excel(f's3://{s_bucket}/{file_path}/', header=1, skipfooter=0)
    raw_df.columns = ['status', 'licenseNumber','entityName','city','state','postalCode','firstName', 'lastName', 'phone']
    raw_df[['status']] = raw_df[['status']].fillna('No')
    raw_df = raw_df.fillna('NA')
    raw_df[['status']] = raw_df[['status']].replace('ü', 'Yes')
    print(raw_df)
    with s3.open(f's3://{d_bucket}/{d_file}/','w') as f:
        raw_df.to_csv(f, index=False)


date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
filenames = ['cultivation_facility', 'dispensary_facility', 'infused_product_manufacturing_facility',
             'laboratory_testing_facility', 'transportation_facility']
for file in filenames:
    try:
        source_filename = 'US/MO/CannabisLifecycle/' + file + '_' + date + '.xlsx'
        dest_filename = 'US/MO/CannabisLifecycle/' + file + '_' + date + '.csv'
        p_data(source_bucket, source_filename, dest_bucket, dest_filename)
        print("Processing has been successfully completed")
    except:
        print("Processing Failed! Please check the source.")

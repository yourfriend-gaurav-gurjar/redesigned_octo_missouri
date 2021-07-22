import pandas as pd
from datetime import datetime, timedelta
import boto3
import s3fs
import awswrangler as wr

source_bucket = 'cannaspyglass-datalake-processed-dev'
dest_bucket = 'cannaspyglass-datalake-enriched-dev'
s3client = boto3.client('s3', region_name='us-east-1')

li = []


def enrich_data(s_bucket, file_path, d_bucket, d_file, license_type):
    s3 = s3fs.S3FileSystem(anon=False)
    processed_df = pd.read_csv(f's3://{s_bucket}/{file_path}/')
    processed_df['license_type'] = license_type
    print(processed_df)
    # processed_df = processed_df.fillna('NA')
    # processed_df[['license_type']] = processed_df[['license_type']].str.capitalize()
    with s3.open(f's3://{d_bucket}/{d_file}/', 'w') as f:
        processed_df.to_csv(f, index=False)


date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
filenames = ['cultivation_facility', 'dispensary_facility', 'infused_product_manufacturing_facility',
             'laboratory_testing_facility', 'transportation_facility']

for file in filenames:
    try:
        source_filename = 'US/MO/CannabisLifecycle/' + file + '_' + date + '.csv'
        dest_filename = 'US/MO/LicenseInfo/' + file + '_' + date + '.csv'
        # merge_data(source_bucket, source_filename, dest_bucket, dest_filename)
        enrich_data(source_bucket, source_filename, dest_bucket, dest_filename, file)
        print("Enrichment has been successfully completed")
    except:
        print("Enrichment Failed! Please check the processing source.")

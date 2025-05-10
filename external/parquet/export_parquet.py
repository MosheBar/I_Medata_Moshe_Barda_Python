import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from sqlalchemy import create_engine

# 1) Point at your Postgres instance and schema
# import os
# from sqlalchemy import create_engine

user = 'postgres'
pw   = 'P7088990p!Postgres'
host = 'database-1.cebui844m7jo.us-east-1.rds.amazonaws.com'
port = '5432'
db   = 'postgres'

engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{db}')
#engine = create_engine('postgresql://user:pass@host:5432/postgres')

# 2) Prepare your S3 client and target bucket
s3 = boto3.client('s3')
bucket = 'external-medate-exam-data'
prefix = 'parquet'

# 3) Export each table to Parquet and upload
for table in ['admissions', 'lab_results', 'lab_tests', 'patient_information']:
    # Read from the 'medate_exam' schema
    df = pd.read_sql_table(table, engine, schema='medate_exam')

    # Convert to a PyArrow table and write to a Parquet file locally
    table_pa = pa.Table.from_pandas(df)
    local_path = f'{table}.parquet'
    pq.write_table(table_pa, local_path)

    # Upload that file into S3 under parquet/<table>/<table>.parquet
    s3_key = f'{prefix}/{table}/{table}.parquet'
    print(f'Uploading {local_path} to s3://{bucket}/{s3_key}')
    s3.upload_file(local_path, bucket, s3_key)

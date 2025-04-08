import boto3
import pandas as pd
from io import StringIO

import snowflake.connector 
from getpass import getuser

def sf_conn():
    conn = snowflake.connector.connect(
        user=getuser(),
        account='wwgrainger.us-east-1',
        password='',
        authenticator="externalbrowser",
        token='',
        warehouse='GSCCE_ETL_S',
        database='',
        schema='',
        role='GSCCE'
    )
    return conn

def import_sf_data(conn, file, stage, table):
    cur = conn.cursor()
    #cur.execute(f'CREATE OR REPLACE STAGE {stage}')
    cur.execute(f'PUT file://{file} @{stage}')

    cur.execute(f"""COPY INTO {table} FROM @{stage}  
        FILE_FORMAT = (TYPE = 'CSV',
            FIELD_DELIMITER = ',',
            SKIP_HEADER = 1,
            TRIM_SPACE = TRUE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"', 
            NULL_IF = ('null', 'nul', 'None', '', ' ', '?', '-', 'Nat', 'NaN', 'nan'))"""
        )

def latest_file_key(client, bucket_name, prefix_name):
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_name)
    latest_file = max(response['Contents'], key=lambda x: x['LastModified']) # Get the latest CSV file's key (file name)
    return latest_file['Key']

def get_csv(client, bucket_name, file_key):
    response = client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    csv_file = StringIO(file_content.decode('utf-8'))  # Decoding from bytes to string
    df = pd.read_csv(csv_file).dropna(how='all')

    csv_buffer = StringIO() # using memory instead of creating a physical csv file
    df.to_csv(csv_buffer, index=False)
    return csv_buffer


session = boto3.Session(profile_name="aad-mlops-nonprod")
s3_client = session.client("s3")
#response = s3_client.list_buckets()
#print(response)


bucket_name = "grainger-mlops-gscce-dev"
prefix_name = "snowflake-stage/autosto/gus-csv"
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_name)

latest_file = max(response['Contents'], key=lambda x: x['LastModified']) # Get the latest CSV file's key (file name)

latest_file_key = latest_file['Key']
print(f"Latest CSV file: {latest_file_key}")


# Read the CSV data directly into memory
response = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
file_content = response['Body'].read()

csv_file = StringIO(file_content.decode('utf-8'))  # Decoding from bytes to string

df = pd.read_csv(csv_file).dropna(how='all')
print(df.head(10)) #


csv_buffer = StringIO() # using memory instead of creating a physical csv file
df.to_csv(csv_buffer, index=False)

if __name__ == '__main__':
    conn = sf_conn()
    bucket_name = "grainger-mlops-gscce-dev"
    session = boto3.Session(profile_name="aad-mlops-nonprod")
    s3_client = session.client("s3")
    prefix_gus = "snowflake-stage/autosto/gus-csv"
    prefix_gcan = "snowflake-stage/autosto/gcan-csv"
    table = "GSCCE.SANDBOX.AUTO_STO"
    stage = "GSCCE.SANDBOX.AUTO_STO_STAGE"

    gus_key = latest_file_key(s3_client, bucket_name, prefix_gus)
    gcan_key = latest_file_key(s3_client, bucket_name, prefix_gcan)
    conn.close()
# %%

import json
import boto3
import os

data_dir = os.path.abspath('../data/')
tb_list = [os.path.join(data_dir, file) for file in os.listdir(data_dir)]

with open('aws_keys.json', 'r') as file:    
    credentials = json.load(file)

def save_s3(tb, s3_client):
    s3_path = f'raw/gc/full-load/{os.path.basename(tb)}/full_load.csv'

    s3_client.upload_file(tb, 'platform-datalake-databrun', s3_path)
    
    return s3_client

s3_client = boto3.client('s3',
                         aws_access_key_id=credentials['access_key'],
                         aws_secret_access_key=credentials['secret_key'],
                         region_name='sa-east-1')


# %%
for tb in tb_list:
    s3_client = save_s3(tb, s3_client)

# %%


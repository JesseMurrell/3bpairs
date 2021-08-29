import os
import re

import boto3 
from botocore import UNSIGNED
from botocore.client import Config

from utils.image_processing import build_image_db_entries
from utils.docker import (
    write_dataframe_to_postgres, generate_postgres_connection_str)

print(f'Beginning postgres database population')
mnist_images_s3_path = os.environ['IMAGES_S3_LOCATION']
image_sample_size = int(os.environ['IMAGE_SAMPLE_SIZE'])
table_name = os.environ['POSTGRES_TABLE_NAME']

anonymous_s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED))

print('Building image dataset...')
images_dataframe = build_image_db_entries(
    s3_full_url=mnist_images_s3_path,
    image_sample_size = 10,
    s3_client=anonymous_s3_client)

print('Dataset generated. Populating database')
write_dataframe_to_postgres(images_dataframe, table_name)

query_url = generate_postgres_connection_str()
query_url = re.sub('postgres_db', 'localhost', query_url)
print(f'Database populated, access via\n\
$ psql {query_url}')
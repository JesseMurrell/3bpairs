import os

import boto3 
from botocore import UNSIGNED
from botocore.client import Config

from utils.docker import write_dataframe_to_postgres
from utils.image_processing import build_image_db_entries

print('hello world')

mnist_images_s3_path = os.environ['IMAGES_S3_LOCATION']
image_sample_size = os.environ['IMAGE_SAMPLE_SIZE']

anonymous_s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED))

images_dataframe = build_image_db_entries(
    s3_full_url=mnist_images_s3_path,
    image_sample_size = 100,
    s3_client=anonymous_s3_client)

write_dataframe_to_postgres(images_dataframe, 'test_table')


print('done')
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
    image_sample_size = image_sample_size,
    s3_client=anonymous_s3_client)

print('Dataset generated. Populating database')
write_dataframe_to_postgres(images_dataframe, table_name)

query_url = generate_postgres_connection_str()
query_url = re.sub('postgres_db', 'localhost', query_url)

print(f"""\
Database poplulated. The image sample can now be accessed.

Postgres:
    - Access in the terminal using the command:
    $ psql {query_url}

Python:
    - Access from this directory using the commmand:
    $ docker run --env-file=.env -it python:3.8 /bin/bash

    Note the .env file must be referenced in the commmand to accesss the
    database.

    To iterate through the image collection start a python interactive 
    session and import the image_iterator() generator function from the
    utils.image_iterator module. Example usage:

    $ docker run --env-file=.env -it python:3.8 /bin/bash
    $ python3 
    >>> from utils.image_iterator import get_random_images
    >>> image_iterator = get_random_images()
""")
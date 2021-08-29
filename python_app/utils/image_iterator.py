
# from dotenv import load_dotenv
# load_dotenv()
import os
import re
import ast
from collections.abc import Iterable

import psycopg2
import numpy as np
import pandas as pd

from utils.image_processing import get_image_matrix
from utils.docker import get_postgres_env_credentials


def postgres_db_to_dataframe(
    table_name : str = os.environ['POSTGRES_TABLE_NAME']) -> pd.DataFrame:
    """
    Queries a docker postgres image defined in the enviromental variables 
    and returns a pandas DataFrame of the data.csv

    :param table_name: The postgres table to be queried and returned as a 
    dataframe.

    :return pd.DataFrame: Postgres query as DataFrame.
    """
    credentials = get_postgres_env_credentials()
    
    try:
        credentials['host'] = 'localhost'
        conn_rwh = psycopg2.connect(**credentials)
        query = f" select * from {table_name};"
        progress_df = pd.read_sql(query, con=conn_rwh)

    except psycopg2.OperationalError:
        credentials = get_postgres_env_credentials()
        credentials['host'] = 'host.docker.internal'
        conn_rwh = psycopg2.connect(**credentials)
        query = f" select * from {table_name};"
        progress_df = pd.read_sql(query, con=conn_rwh)

    conn_rwh.close()

    return progress_df


def get_random_images(
    response_item_type : str = 'pixel_array') -> Iterable:
    """
    Generator function to iterate over image data hosted in docker postgres
    container. Credentials are pulled from environmental variables.

    :param response_item_type: The type of object that is returned by the
    function. Defaults to 'pixel_array' which returns the pixel matrix from 
    the image. Alternate arg is 'all_data' which returns all row data on the 
    image from the database as a dict. 
    """

    image_sample_df = postgres_db_to_dataframe()

    for index, row in image_sample_df.iterrows():

        if response_item_type == 'pixel_array':
            pixel_array =  ast.literal_eval(row['pixel_array'])
            yield pixel_array

        elif response_item_type == 'all_data':
            item_data = row.to_dict()
            item_data['pixel_array'] = ast.literal_eval(
                item_data['pixel_array'])
            yield item_data
        
        else:
            raise ValueError("Please specift a valid response_item_type. Valid\
                args: ['pixel_array', 'all_data']")
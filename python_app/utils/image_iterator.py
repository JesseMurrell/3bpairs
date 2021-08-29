
# from dotenv import load_dotenv
# load_dotenv()
import os
import re
import ast

import psycopg2
import numpy as np
import pandas as pd
from utils.docker import get_postgres_env_credentials
from utils.image_processing import get_image_matrix

def postgres_db_to_dataframe(
    table_name : str = os.environ['POSTGRES_TABLE_NAME']):

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

def format_postgres_array(
    postgres_formatted_array):

    array_replace_bracket_1 = re.sub('{','[',postgres_formatted_array)
    array_replace_all_brackets = re.sub('}',']',array_replace_bracket_1)
    formatted_array = ast.literal_eval(array_replace_all_brackets)

    return formatted_array

def image_sample_iterator(
    response_item_type : str = 'pixel_array') -> 'Generator[np.ndarray]':

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
        
image_iterator = image_sample_iterator()

# from utils.image_iterator import image_iterator
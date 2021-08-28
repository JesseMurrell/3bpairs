import os
import logging

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
print("Hello World")

logger = logging.getLogger(__name__)

def get_postgres_env_credentials():

    credentials = {
        'database' : os.environ['POSTGRES_DB'],
        'user' : os.environ['POSTGRES_USER'],
        'password' : os.environ['POSTGRES_PASSWORD'],
        'host' : 'postgres_db',
        'port' : os.environ['POSTGRES_PORT'],
    }
    return credentials

def generate_postgres_connection_str(
    credentials : str = None ) -> str:

    if credentials == None:
        credentials = get_postgres_env_credentials()

    connection_str = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        **credentials)

    return connection_str

def write_dataframe_to_postgres(
    dataframe : pd.DataFrame,
    table_name : str) -> str:

    logger.info('Creating Connection String')
    postgress_connection = generate_postgres_connection_str()
    logger.info('Making connection to database')
    postgress_connection_str = generate_postgres_connection_str()
    engine = create_engine(postgress_connection_str)
    logger.info(f'Connection made. Uploading dataframe as {table_name} table')
    dataframe.to_sql(table_name, engine)
    logger.info(f'Table {table_name} uploaded.')
    
    return 'Upload Complete'

x = [1,2,3]
y = [4,5,6]
df = pd.DataFrame({'x' : x, 'y' : y })
write_dataframe_to_postgres(
    dataframe=df,
    table_name='test_table3')

print('done')

# conn_str = "postgresql+psycopg2://3bpairs:password@postgres_db:5432/3bpairs-database"

# # ? make table
# import pandas as pd
# x = [1,2,3]
# y = [4,5,6]
# df = pd.DataFrame({'x' : x, 'y' : y })
# postgress_conn = "postgres://3bpairs:password@localhost:5432/3bpairs-database"
# engine = create_engine(postgress_conn)
# df.to_sql('test_table', engine)


# # ? query table
# import psycopg2
# conn_rwh = psycopg2.connect(database='3bpairs-database', user='3bpairs', password='password',
#                             host='localhost', port='5432')
# query = \
# '''
# select * from test_table;
# '''
# output = pd.read_sql(query, con=conn_rwh)
# print(output.shape)
# output.head(5)
# # end connection
# conn_rwh.close()
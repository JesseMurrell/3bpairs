import os
import logging

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def get_postgres_env_credentials() -> dict:
    """
    Returns the docker postgres credentials as a dict. These credentials are
    defined as enviromental variables in the python enviroment (generally 
    defined in the .env file).
    """
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
    """
    Generates a postgresql connection string using either credentials defined 
    as an input variable or via the credentials defined in the python
    enviroment (default). 

    :param credentials: Credentials for postgres connection string. Required
    args = [
        'user','password','host','post','database'
    ]
    """
    if credentials == None:
        credentials = get_postgres_env_credentials()

    connection_str = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        **credentials)

    return connection_str

def write_dataframe_to_postgres(
    dataframe : pd.DataFrame,
    table_name : str,
    credentials : str = None) -> str:
    """
    Writes a pandas dataframe to a postgres database table.
    
    :param dataframe: Pandas dataframe to be created as table in postgres 
    database.
    :param table_name: The name of the table in the postgres database
    :param credentials (optional): Credentials for postgres connection string. 
    Defaults to credentials specied in the enviromental variables. Required
    args = [
        'user','password','host','post','database'
    ]

    :return: Confirmation upload was made succesfully.
    """

    logger.info('Creating Connection String')
    postgress_connection = generate_postgres_connection_str()
    logger.info('Making connection to database')
    postgress_connection_str = generate_postgres_connection_str()
    engine = create_engine(postgress_connection_str)
    logger.info(f'Connection made. Uploading dataframe as {table_name} table')
    dataframe.to_sql(table_name, engine, if_exists='replace',index_label='id')
    logger.info(f'Table {table_name} uploaded.')
    
    return 'Success! Upload Complete.'
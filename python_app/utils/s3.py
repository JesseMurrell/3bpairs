import re

import boto3
from tqdm import tqdm


def get_s3_path_components(
    full_s3_url : str) -> dict:
    """
    Returns a dictionary of the various components of an s3 url 
    beginning with 's3://'. The components returned are 'bucket',
    'path' and 'file'. If a specific component is not detected the 
    dictionary will return none for that key, i.e {'file' : None}.

    :param full_s3_url: Full s3 path to file, i.e,
    s3://bucket/directory_1/directory_2/file.csv.

    :return : dictionary with 3 keys, 'bucket', 'path', and 'file'.
    """

    regex_bucket_pattern = 's3:\/\/([A-Za-z0-9-]*)'
    s3_bucket = re.search(
        regex_bucket_pattern, full_s3_url)[1]

    try:
        regex_bucket_path_pattern = 's3:\/\/.*?\/(.*)'
        s3_bucket_path = re.search(
            regex_bucket_path_pattern, full_s3_url)[1]
    except TypeError:
        s3_bucket_path = None

    try:
        regex_file_pattern = '.*\/(.*)'
        s3_file_name = re.search(
            regex_file_pattern, full_s3_url)[1]
        #checking is actually a file
        if '.' not in s3_file_name:
            s3_file_name = None
    except TypeError:
        s3_file_name = None

    s3_path_structure = {
        'bucket' : s3_bucket,
        'path' : s3_bucket_path,
        'file' : s3_file_name
    }
    return s3_path_structure


def compose_full_s3_url(
    bucket_name : str,
    bucket_path : str) -> str:

    full_s3_path = 's3://' + bucket_name + '/' + bucket_path
    return full_s3_path


def list_s3_path_files(
    full_s3_url : str,
    return_last_modified : bool = False,
    s3_client : boto3.client = None) -> list:
    """
    Lists all files within an S3 path.

    :param full_s3_url: The full s3 file path, i.e, 
    s3://bucket/directory_1/directory_2
    :param return_last_modified: Where the last modified data of
    the s3 files are returned. If return_last_modified=True the
    items within the return list will be dictionaries rather 
    than strings.
    :param s3_client: Active s3 client object.

    :return : List of s3 file paths within the specified s3 path.
    """
    if full_s3_url[-1] != '/':
        full_s3_url = full_s3_url + '/'

    s3_path_components = get_s3_path_components(full_s3_url)
    bucket_name = s3_path_components['bucket']
    bucket_path = s3_path_components['path']

    if s3_client == None:
        s3_client = boto3.client('s3')
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket = bucket_name,
        Prefix = bucket_path)

    all_bucket_objects = []
    for page in pages:
        for s3_object in page['Contents']:
            all_bucket_objects.append(s3_object)

    file_list = []
    for bucket_object in all_bucket_objects:
        composed_s3_url = compose_full_s3_url(
            bucket_name, bucket_object['Key'])

        if composed_s3_url != full_s3_url:
            if return_last_modified == True:
                s3_file_object = {
                    'file' : composed_s3_url,
                    'last_modified' : bucket_object['LastModified']
                }
            else:
                s3_file_object = composed_s3_url
            
            file_list.append(s3_file_object)
    
    return file_list


def download_file_from_s3(
    bucket_name : str,
    relative_s3_file_path : str ,
    saved_file_name : str,
    s3_client : boto3.client = None):
    """
    Downloads a file from AWS and save locally. 

    :param bucket_name: The name of the bucket in which to look for the file.
    :param file_path: The full path of the file within the bucket.
    :param saved_file_name: The name of the file when saved to locally.
    :param s3_client: Active s3 client object.

    :return: None
    """
    if s3_client == None:
        s3_client = boto3.client('s3')

    s3_client.download_file(bucket_name, relative_s3_file_path, saved_file_name)


def download_list_of_files_from_s3(
    s3_file_paths_list: list,
    local_directory: str = '/tmp',
    s3_client : boto3.client = None) -> list:
    """
    Downloads a list of files from stored in AWS s3.

    :param s3_file_paths_list : List of full s3 url paths
    :param local_directory : Local save directory of the files.
    Defaults to '/tmp'.

    :return : List of the path to each file saved.
    """
    files_saved = []
    for num, s3_path in enumerate(s3_file_paths_list):
        if num%50 == 0 and num != 0:
            print(f'{num} of {len(s3_file_paths_list)} complete')

        path_components = get_s3_path_components(s3_path)
        bucket_name = path_components['bucket']
        bucket_path = path_components['path']
        saved_file_name = path_components['file']

        local_save_path = local_directory + '/' + saved_file_name

        saved_files = []
        download_file_from_s3(
            bucket_name,
            bucket_path,
            local_save_path,
            s3_client
        )
        files_saved.append(local_save_path)
    
    print('Download Complete')
    return files_saved


def get_file_s3_folder(
    full_s3_url : str) -> str:
    """
    Gets the lowest level folder for a file store in s3. Function
    assumes the full_s3_url is for a file.

    
    :param full_s3_url: Full s3 path to file, i.e,
    s3://bucket/directory_1/directory_2/file.csv.

    :return return: Lowests level s3 folder.
    """

    s3_url_components = full_s3_url.split('/')
    s3_folder = s3_url_components[-2]
    return s3_folder



    
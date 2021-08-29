import re
import random
import datetime

import boto3
import numpy as np
import pandas as pd
from PIL import Image

from utils.s3 import (
    get_file_s3_folder,
    list_s3_path_files,
    get_s3_path_components,
    download_list_of_files_from_s3)

def translate_image_mode(
    mode : str) -> str:
    """
    Converts the image mode from the Pil module from a 
    letter, i.e L or P, to a more descriptive phrase.

    :param mode: Mode image ouput from Pil module.

    :return str: Mode as word.
    """
    image_mode_key = {
        'L' : 'Greyscale',
        'P' : 'Palettised',
        'RGB' : "RGB"
    }
    mode_as_text = image_mode_key[mode.upper()]
    return mode_as_text


def get_image_matrix(
    image_path : str ) -> np.ndarray:
    """
    Returns the pixel matrix from an image as a numpy array.

    :param image_path: Local path to input images

    :return np.ndarray: Image pixel matrix as a an array.
    """
    image = Image.open(image_path)
    image_array = np.asarray(image)

    return image_array


def get_image_data(
    image_path : str) -> dict:
    """
    Builds a dictionary of data on an image from a specified local image path.
    
    :param image_path: Local image path.

    :return dict : Dictionary of information of the image. Fields [
        'pixel_array', 'shape' ,'mode' 'size_x', 'size_y'
    ]
    """
    image = Image.open(image_path)
    image_array = np.asarray(image)
    image_mode = translate_image_mode(image.mode)
    image_size_x = image.size[0]
    image_size_y = image.size[1]
    image_shape = image_array.shape

    image_data = {
        'pixel_array' : image_array,
        'shape' : image_shape,
        'mode' : image_mode,
        'size_x' : image_size_x,
        'size_y' : image_size_y
    }

    return image_data


def get_s3_image_data(
    local_image_path : str,
    s3_image_path : str ,
    image_upload_date : datetime.datetime) -> dict:
    """
    Builds a dictionary of data on a specified image that is being stored 
    locally but has been downloaded from AWS s3.

    :param local_image_path: Where image is stored in the local directory.
    :param s3_image_path: Where the image has been downloaded from is AWS s3.
    :image_upload_date : The date an image was uploaded to AWS s3.
    """

    image_name = local_image_path.split('/')[-1]
    image_title = image_name.split('.')[0]
    image_format = local_image_path.split('.')[-1]
    image_data = get_image_data(local_image_path)
    image_label = get_file_s3_folder(s3_image_path)
    time_now = datetime.datetime.now()
    image_data['label'] = image_label
    image_data['title'] = image_title
    # image_data['local_path'] = local_image_path
    image_data['s3_path'] = s3_image_path
    image_data['image_format'] = image_format
    image_data['s3_upload_date'] = image_upload_date.replace(tzinfo=None)
    image_data['date_added_to_db'] = time_now

    return image_data


def build_image_db_entries(
        s3_full_url : str,
        image_sample_size : int,
        s3_client : boto3.client('s3')) -> pd.DataFrame:
    """
    Creates a pandas dataframe from a random sample of images stored in AWS s3. 

    :param s3_full_url: Directory of the image filses in s3.
    :param image_sample_size: The number images to be included included in the
    dataframes random sample.
    :param s3_client: Active s3 client object.

    :return pd.DataFrame: Pandas dataframe of data from images pulled from AWS
    s3.
    """
    s3_bucket_items = list_s3_path_files(
        s3_full_url,
        return_last_modified = True,
        s3_client=s3_client)

    s3_bucket_files = [
        image for image in s3_bucket_items if '.' in image['file']]
        
    sample_files_s3_metadata = random.choices(
        s3_bucket_files, k = image_sample_size)

    s3_image_paths = [
        img['file'] for img in sample_files_s3_metadata
    ]

    local_paths = download_list_of_files_from_s3(
        s3_image_paths,
        s3_client=s3_client)

    sample_image_data = []
    for image_s3_index, local_image in enumerate(local_paths):
        image_s3_path = s3_image_paths[image_s3_index]
        image_upload_date = sample_files_s3_metadata[image_s3_index]['last_modified']

        image_item_data = get_s3_image_data(
            local_image,
            image_s3_path,
            image_upload_date)
        
        sample_image_data.append(image_item_data)

    images_dataframe = pd.DataFrame(sample_image_data) 
    # make array sqlachemy safe
    array_replacements = {'{' : '[', '}' : ']'}
    translation_dict = str.maketrans(array_replacements)
    images_dataframe['pixel_array'] = images_dataframe.pixel_array.apply(
        lambda x : str(x.tolist()).translate(translation_dict))

    print(images_dataframe.columns)
    return images_dataframe
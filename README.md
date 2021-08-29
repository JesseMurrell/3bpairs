# 3bpairs Classifier Enviroment

Creates a docker container with both a postgresql database and python environment for data scientists to iterate through a random sample of 1000 images(of a greater set of roughly 10,000 stored in AWS). The project is designed to train a classifier with the ability of increasing the image sample from 1000 if required.


## Prerequisites 

Docker - [install](https://docs.docker.com/get-docker/)

Docker Compose (May need to install separately to Docker depending on system) - [install](https://docs.docker.com/compose/install/)


## Usage

### Set Up
The container is built using the [`docker-compose.yaml`](https://github.com/JesseMurrell/3bpairs/blob/main/docker-compose.yml) file. The file pulls the a lot of it's set up from a `.env` file stored in the root directory. I have included an example `.env` file in the root directory to get you started but feel free to modify as required.

Note* If you have postgres instances running on port 5432 you may need to modify the `POSTGRES_PORT` variable in your `.env` file.


### Running
The Docker Environment is composed of two images:

* Postgres: A PostgresSQL 11 database is used to store the data for querying and iteration via python. 

* Python: Python3.8 image used for processing and exploration. The medical images from [https://www.kaggle.com/andrewmvd/medical-mnist](https://www.kaggle.com/andrewmvd/medical-mnist) are stored in AWS S3. The medical images & their metadata are pulled from S3 via python and written into the postgres database.

Specify the image sample size in your `.env` file as
```
IMAGE_SAMPLE_SIZE=1000
```

Build image with 
```
$ docker-compose build
```
Run with
```
$ docker-compose up
```
I advise running as specified above for progress prompts on the population of the database and docker command support, however to run headless (Detached) use
```
$ docker-compose up -d
```
### Access
When running `$ docker-compose up` you will be prompted with commands for accessing both the postgres and python environments. 

You can generally access the postgres database using the below syntax in the terminal:

```
$ psql postgresql://<POSTGRES_USER>:<POSTGRES_PASSWORD>@localhost:<POSTGRES_HOST>/
```

From within the python image you can iterate through the medical image sample through importing the image_iterator() generator function from the utils.image_iterator module. Example usage:

```
    $ docker run --env-file=.env -it python:3.8 /bin/bash
    $ python3 
    >>> from utils.image_iterator import image_sample_iterator
    >>> image_iterator = get_random_images()
    >>> print(next(image_iterator))
    # image array
```
Note* the .env file must be imported into your python image/environment. Also, if you are unable to run the python image as shown above you may need to replace the `python:3.8` section in the snippet with the image id.

The `get_random_images` function takes a single argument, `response_item_type ='pixel_array'` by default. There is also another valid value for  response_item_type,`response_item_type ='all_data'`. 

* `response_item_type ='pixel_array' : Yields a matrix as a python array of the pixels for the current iteration image.
* `response_item_type ='all_data' : Returns all data for the current interation image as a dict, dict include 'pixel_array' as a key.

## Authors

Jesse Murrell


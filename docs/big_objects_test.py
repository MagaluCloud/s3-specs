import pytest
import logging
from utils.utils import create_big_file
from utils.crud import bucket_with_name
from boto3.s3.transfer import TransferConfig
import uuid
from tqdm import tqdm
import os

size_list = [
            {'size': 10, 'unit': 'mb'},
            {'size': 100, 'unit': 'mb'},
            {'size': 1, 'unit': 'gb'},
            {'size': 5, 'unit': 'gb'},
            {'size': 10, 'unit': 'gb'},
]   

@pytest.mark.parametrize(
    'size',
    [s for s in size_list],
    ids=[f"{s['size']}{s['unit']}" for s in size_list]
)


@pytest.mark.turtle
@pytest.mark.big_objects
def test_multipart_upload(s3_client, bucket_with_name, size):
    """
    Test to upload a big object to an S3 bucket using multipart upload
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :param size: dict: value containing an int size and a string unit
    :return: None
    """
    file_path = f"./tmp_files/big_file_upload_{size['size']}{size['unit']}"
    bucket_name = bucket_with_name
    size_file = create_big_file(file_path, size)

    object_key = "big_object_" + uuid.uuid4().hex[:6]
    
    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=100 * 1024 * 1024,
        use_threads=True
    )

    try:
        # Upload Progress Bar with time stamp
        with tqdm(total=size_file, 
                  desc=bucket_name, 
                  bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | Time: {elapsed} | {desc}",  
                  unit='B', 
                  unit_scale=True, unit_divisor=1024) as pbar:
            
            response = s3_client.upload_file(file_path, bucket_name, object_key, Config=config,  Callback=pbar.update)  
            elapsed = pbar.format_dict['elapsed']

            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            object_size = response.get('ContentLength', 0)
            assert object_size == size_file, "Uploaded object size doesn't match"
        
        logging.error(f"Object: {object_key}, size: {object_size}, bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")


@pytest.mark.parametrize(
    'size',
    [s for s in size_list],
    ids=[f"{s['size']}{s['unit']}" for s in size_list]
)

@pytest.mark.turtle
@pytest.mark.big_objects
def test_multipart_download(s3_client, bucket_with_name, size):
    """
    Test to download a big object to an S3 bucket using multipart download
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :param size: dict: value containing an int size and a string unit
    :return: None
    """

    file_path = f"./tmp_files/big_file_download{size['size']}{size['unit']}"
    bucket_name = bucket_with_name
    size_file = create_big_file(file_path, size)
    object_key = "big_object" + uuid.uuid4().hex[:6]

    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    # upload object to s3 
    try:
        with tqdm(total=size_file, 
                  desc=bucket_name, 
                  bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}",  
                  unit='B', 
                  unit_scale=True, unit_divisor=1024) as pbar:

            s3_client.upload_file(file_path, bucket_name, object_key, Config=config,  Callback=pbar.update)  
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")



    # Test download file from s3 bucket
    try:
        download_path = file_path + '_downloaded'

        with tqdm(total=size_file, 
                  desc=bucket_name, 
                  bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}",  
                  unit='B', 
                  unit_scale=True, unit_divisor=1024) as pbar:

            response = s3_client.download_file(Bucket=bucket_name, Key=object_key, Filename = download_path, Config=config, Callback=pbar.update)  
        try:
            assert os.path.getsize(download_path) == size_file, "Download size doesnt match file size"
        finally:
            if os.path.exists(download_path):
                os.remove(download_path)

    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")

 
import pytest
import logging
from utils.utils import (create_big_file)
from utils.crud import bucket_with_name
from boto3.s3.transfer import TransferConfig
import uuid

size_list = [{'size': 10, 'unit': 'mb'},
 #           {'size': 100, 'unit': 'mb'},
#            {'size': 1, 'unit': 'gb'},
#            {'size': 5, 'unit': 'gb'},
#            {'size': 10, 'unit': 'gb'},
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
    file_path = "./big_file.txt"
    bucket_name = bucket_with_name
    size = create_big_file(file_path, size)

    object_key = "big_object" + uuid.uuid4().hex[:6]
    
    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    try:
        response = s3_client.upload_file("./big_file", bucket_name, object_key, Config=config)  
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logging.error(f"Uploaded object: {object_key} to bucket: {bucket_name}")
        object_size = response['ContentLength']
        logging.error(f"Size of the object: {object_size} bytes")
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")


#@pytest.mark.parametrize(
#    'size',
#    [s for s in size_list],
#    ids=[f"{s['size']}{s['unit']}" for s in size_list]
#)
#
#
#@pytest.mark.turtle
#@pytest.mark.big_objects
#def test_multipart_download(s3_client, bucket_with_name, size):
#    """
#    Test to download a big object to an S3 bucket using multipart download
#    :param s3_client: fixture of boto3 s3 client
#    :param bucket_with_name: fixture to create a bucket with a unique name
#    :param size: dict: value containing an int size and a string unit
#    :return: None
#    """
#
#    bucket_name = bucket_with_name
#    big_file_path = create_big_file(size)
#    object_key = "big_object" + uuid.uuid4().hex[:6]
#
#    # Config for multhreading of boto3 building multipart upload/download
#    config = TransferConfig(
#        multipart_threshold=8 * 1024 * 1024,
#        max_concurrency=10,
#        multipart_chunksize=8 * 1024 * 1024,
#        use_threads=True
#    )
#
#    # upload object to s3
#    try:
#        response = s3_client.upload_file(big_file_path.name, bucket_name, object_key, Config=config)  
#        assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Expected a 200 response code"
#
#    except Exception as e:
#        logging.error(f"Error uploading object {object_key}: {e}")
#
#    # Test download file from s3 bucket
#    try:
#        response = s3_client.download_file(big_file_path.name, bucket_name, object_key, Config=config)  
#        assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Expected a 200 response code"
#    except Exception as e:
#        logging.error(f"Error uploading object {object_key}: {e}")
#
# 
import pytest
import logging
from utils.utils import (create_big_file)
from utils.crud import bucket_with_name
from boto3.s3.transfer import TransferConfig
import uuid


@pytest.mark.turtle
@pytest.mark.big_objects
def test_multipart_upload(s3_client, bucket_with_name):
    """
    Test to upload a big object to an S3 bucket using multipart upload
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :return: None
    """
    file_path = ".../bin/big_file"
    bucket_name = bucket_with_name

    size = create_big_file(file_path, size=100, unit='MB')
    object_key = "big_object" + size + uuid.uuid4().hex[:6]
    bucket_name = bucket_with_name
    
    create_big_file(file_path, size=100, unit='MB')

    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    try:
        response = s3_client.upload_file(file_path, bucket_name, object_key, Config=config)  
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Expected a 200 response code"


def test_multipart_download(s3_client, bucket_with_name):
    """
    Test to download a big object to an S3 bucket using multipart download
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :return: None
    """
    file_path = ".../bin/big_file"
    bucket_name = bucket_with_name

    size = create_big_file(file_path, size=100, unit='MB')
    object_key = "big_object" + size + uuid.uuid4().hex[:6]


    bucket_name = bucket_with_name
    create_big_file(file_path, size=100, unit='MB')

    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    # upload object to s3
    try:
        response = s3_client.upload_file(file_path, bucket_name, object_key, Config=config)  
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")

    # Test download file from s3 bucket
    try:
        response = s3_client.download_file(file_path, bucket_name, object_key, Config=config)  
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Expected a 200 response code"
 
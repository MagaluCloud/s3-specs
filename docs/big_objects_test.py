import pytest
import logging
from utils.utils import create_big_file
from utils.crud import (upload_objects_multithreaded,
                        bucket_with_name)



@pytest.mark.turtle
@pytest.mark.big_objects
def multipart_upload(s3_client, bucket_with_name):
    """
    Test to upload a big object to an S3 bucket using multipart upload
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :return: None
    """

    bucket_name = bucket_with_name
    object_key = "big-object"
    file_path = "/tmp/big_file"
    create_big_file(file_path, size=100, unit='MB')

    try:
        response = multipart_upload(s3_client, bucket_name, object_key, file_path)
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")
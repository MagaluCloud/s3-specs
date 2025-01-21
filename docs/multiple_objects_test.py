import pytest
import logging
from utils.crud import (bucket_with_name,
                        upload_multiple_objects,
                        download_objects_multithreaded,
                        list_all_objects)

### Fazendo o upload de grandes quantidades de objetos em paralelo

# O upload de grandes volumes de objetos é demorado, assim o método ideal para esta operação é o paralelismo.
# A função abaixo faz o upload de N objects para um bucket S3 e então os deleta.

# Multiple objects test data
objects_number = [100, 1_000, 10_000, 100_000]
objects_number = [100]
file_path = ["../AUTHORS"]

test_ids = [
    f"num={num}" for num in objects_number
]

@pytest.mark.parametrize(
    'object_quantity, file_path',
    [(num, file_path) for num in objects_number],
    ids=test_ids,
)


@pytest.mark.slow # Mark indicating the test expected speed (slow)
@pytest.mark.multiple_objects
def test_upload_multiple_objects(s3_client, bucket_with_name, file_path: list, object_quantity: int):
    """
    Test to upload multiple objects to an S3 bucket in parallel based on the wanted quantity
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :param object_quantity: int list: number of objects to be uploaded
    :param file_path: str: path to the file to be uploaded
    :param number_threads: int: number of threads to be used in the upload
    :return: None
    """

    bucket_name = bucket_with_name
    object_prefix = "test-multiple-small-"
   
    successful_uploads = upload_multiple_objects(s3_client, bucket_name, file_path, object_prefix, object_quantity)
    objects_in_bucket = len(list_all_objects(s3_client, bucket_name))
    # Checking if all the objects were uploaded

    logging.info(f"Uploaded expected: {object_quantity}, made:{successful_uploads}, bucket: {objects_in_bucket}")
    assert successful_uploads == objects_in_bucket , "Expected all objects to have been successfully uploaded"





@pytest.mark.parametrize(
    'object_quantity, file_path',
    [(num, file_path) for num in objects_number],
    ids=test_ids,
)


@pytest.mark.slow # Mark indicating the test expected speed (slow)
@pytest.mark.multiple_objects
def test_download_multiple_objects(s3_client, bucket_with_name, object_quantity, file_path):
    """
    Test to download multiple objects from an S3 bucket in parallel
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :param number_threads: int: number of threads to be used in the download
    :return: None
    """

    bucket_name = bucket_with_name
    object_prefix = "test-download-small-"

    successful_uploads = upload_multiple_objects(s3_client, bucket_name, file_path, object_prefix, object_quantity)

    logging.info(f"Downloading objects from {bucket_name}")
    successful_downloads = download_objects_multithreaded(s3_client, bucket_name)

    # Checking if all the objects were downloaded
    assert successful_downloads == successful_uploads, "Number of downloads doesnt match the number of objects in the bucket"



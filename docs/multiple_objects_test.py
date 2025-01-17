import pytest
import logging
from utils.crud import (upload_objects_multithreaded,
                        bucket_with_name)
import itertools


### Fazendo o upload de grandes quantidades de objetos em paralelo

# O upload de grandes volumes de objetos é demorado, assim o método ideal para esta operação é o paralelismo.
# A função abaixo faz o upload de N objects para um bucket S3 e então os deleta.

# Multiple objects test data
threads_number = [8, 10, 12]
objects_number = [100, 1_000, 10_000, 100_000]
file_path = "../AUTHORS"
test_data = [(num, threads, file_path) for num in objects_number for threads in threads_number]
test_ids = [
    f"num={num},threads={threads}" for num, threads, _ in test_data
]

@pytest.mark.parametrize(
    'object_quantity, max_threads, body_file',
    test_data,
    ids=test_ids,
)


@pytest.mark.turtle # Mark indicating the test expected speed (slow)
@pytest.mark.muiltiple_objects
def test_upload_multiple_objects(s3_client, bucket_with_name, object_quantity, body_file, max_threads):
    """
    Test to upload multiple objects to an S3 bucket in parallel based on the wanted quantity
    :param s3_client: fixture of boto3 s3 client
    :param bucket_with_name: fixture to create a bucket with a unique name
    :param object_quantity: int: number of objects to be uploaded
    :param body_file: str: path to the file to be uploaded
    :param max_threads: int: number of threads to be used in the upload
    :return: None
    """

    bucket_name = bucket_with_name
    object_prefix = "test-multiple-small-"
    successful_uploads = 0

    # In the case there is only a str as path, we pass it object_quantity times, limiting by the number of threads
    if type(body_file) == str:
        for i in range(0, object_quantity, max_threads):
            objects = [
                {"Key": f"{object_prefix}{i + j}", "Body": body_file} for j in range(max_threads)
            ]
            logging.info(f"Uploading objects {i} to {i + max_threads}")
            successful_uploads = upload_objects_multithreaded(s3_client, bucket_name, objects, max_threads)
    elif type(body_file) == list:
        # In the case we have a list we have to create an infinite iterator so we can cycle through the list
        iter_body_file = itertools.cycle(body_file)

        for i in range(0, object_quantity, max_threads):
            objects = [
                {"Key": f"{object_prefix}{i + j}", "Body": next(iter_body_file)} for j in range(max_threads)
            ]
            logging.info(f"Uploading objects {i} to {i + max_threads}")
            successful_uploads = upload_objects_multithreaded(s3_client, bucket_name, objects, max_threads)

    # Checking if all the objects were uploaded
    assert successful_uploads == object_quantity, "Expected all objects to have been successfully uploaded"
    logging.info(f"Uploaded all {object_quantity} objects to {bucket_name}")







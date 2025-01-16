import pytest
from concurrent.futures import ThreadPoolExecutor
import logging
from utils.crud import (
    bucket_with_name
)


def upload_object(s3_client, bucket_name, object_key, body_file):
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=body_file,
    )
    logging.info(f"Object {object_key} uploaded")
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


threads_number = [16]
objects_number = [999]
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


def test_upload_multiple_objects(s3_client, bucket_with_name, object_quantity, body_file, max_threads):
    bucket_name = bucket_with_name
    object_prefix = "test-multiple-small-"


    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i in range(object_quantity):
            object_key = f"{object_prefix}{i}"
            futures.append(executor.submit(upload_object, s3_client, bucket_name, object_key, body_file))

        for future in futures:
            try:
                future.result()  
            except Exception as e:
                logging.error(f"Error uploading object: {e}")

    response = s3_client.list_objects(Bucket=bucket_name)

    if 'Contents' in response:
        object_count = len(response['Contents'])
    else:
        object_count = 0

    logging.info(f"Total objects uploaded: {object_count}")
    logging.info("Multiple objects uploaded successfully")
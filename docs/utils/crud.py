import logging
import pytest
from concurrent.futures import ThreadPoolExecutor
from s3_helpers import (
    generate_unique_bucket_name,
)


def create_bucket(s3_client, bucket_name):
    # anything different than us-east-1 must have LocationConstraint on aws
    try:
        region = s3_client.meta.region_name
        if (region != "us-east-1"):
            return s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        logging.info(f"Bucket '{bucket_name}' already exists and is owned by you.")
    except s3_client.exceptions.BucketAlreadyExists:
        raise Exception(f"Bucket '{bucket_name}' already exists and is owned by someone else.")
    
    return s3_client.create_bucket(Bucket=bucket_name)


def delete_object(s3_client, bucket_name, object_key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Deleted object: {object_key} from bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Error deleting object {object_key}: {e}")


def delete_bucket(s3_client, bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        logging.info("No such Bucket")
        return
    logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")



def delete_objects_multithreaded(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    objects = response.get("Contents", [])

    if objects:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for obj in objects:
                executor.submit(delete_object, s3_client, bucket_name, obj["Key"])
        logging.info("All delete tasks have been submitted.")
    else:
        logging.info("No objects found in the bucket.")


@pytest.fixture
def bucket_with_name(s3_client, request):
    bucket_name = generate_unique_bucket_name(request.node.name.replace("_", "-"))
    create_bucket(s3_client, bucket_name)

    yield bucket_name

    objects = s3_client.list_objects(Bucket=bucket_name).get("Contents", [])

    delete_objects_multithreaded(s3_client,bucket_name)
    delete_bucket(s3_client, bucket_name)


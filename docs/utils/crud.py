import logging
import pytest
from concurrent.futures import ThreadPoolExecutor
from utils.utils import generate_valid_bucket_name

### Functions

def create_bucket(s3_client, bucket_name):
    """
    Create a new bucket on S3 ensuring that the location is set correctly.
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the to be created bucket
    :return: dict: response from boto3 create_bucket
    """

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



def upload_object(s3_client, bucket_name, object_key, body_file):
    """
    Create a new bucket on S3 ensuring that the location is set correctly.
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the to be created bucket
    :param object_key: str: key of the object
    :param body_file: file: file to be uploaded
    :return: HTTPStatusCode from boto3 put_object
    """

    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=body_file,
        )
        logging.info(f"Object {object_key} uploaded")
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")
 
    return response['ResponseMetadata']['HTTPStatusCode']


def download_object(s3_client, bucket_name, object_key):    
    """
    Download an object from a s3 Bucket
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param object_key: str: key of the object
    :return: HTTPStatusCode from boto3 get_object
    """

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Object {object_key} downloaded")
    except Exception as e:
        logging.error(f"Error downloading object {object_key}: {e}")
    
    return response['ResponseMetadata']['HTTPStatusCode']


# List all objects all at once as opossed to the regular list_objects_v2 which is limited to 1000 objects per request
def list_all_objects(s3_client, bucket_name):
    """
    List all objects in a bucket without size limits
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: list str: names of objects in the bucket
    """

    all_objects = s3_client.resources.Bucket(bucket_name).objects.all()
    return [{"Key": obj.key} for obj in all_objects]


def delete_object(s3_client, bucket_name, object_key):
    """
    Delete an object from a s3 Bucket
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param object_key: str: key of the object
    :return: HTTPStatusCode from boto3 delete_object
    """
        
    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Deleted object: {object_key} from bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Error deleting object {object_key}: {e}")


    return response['ResponseMetadata']['HTTPStatusCode']


def delete_bucket(s3_client, bucket_name):
    """
    Delete a s3 bucket 
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: dict: response from boto3 create_bucket
    """

    try:
        response = s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")
    except s3_client.exceptions.NoSuchBucket:
        logging.info("No such Bucket")
    except s3_client.exceptions.BucketNotEmpty:
        logging.error(f"Bucket '{bucket_name}' is not empty.")

    return response





### Multi-threading

def upload_objects_multithreaded(s3_client, bucket_name, objects_paths, number_threads=10):
    """
    Upload multiple objects to one bucket in parallel
    The number of simultaneous uploads are limited by the number of objects in the list
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param objects: list: list of paths of the objects to be uploaded
    :return: int: number of successful uploads
    """

    successful_uploads = 0

    if objects_paths:
        with ThreadPoolExecutor(max_workers=number_threads) as executor:
            for obj in objects_paths:
                try:
                    executor.submit(upload_object, s3_client, bucket_name, obj["Key"], obj["Body"])
                    successful_uploads = successful_uploads + 1
                except Exception as e: # There is no asserting or raising exceptions, because it is meant to be used in a test
                    logging.error(f"Error uploading object {obj['Key']}: {e}")
        logging.info("All upload tasks have been submitted.")
    else:
        logging.info("No objects found to upload.")

    return successful_uploads


def delete_objects_multithreaded(s3_client, bucket_name, number_threads=10):
    """
    Delete all objects in a bucket in parallel
    :param s3_client: fixture of boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: int: number of successful deletions
    """

    objects = list_all_objects(s3_client, bucket_name)

    deleted = 0

    if objects:
        with ThreadPoolExecutor(max_workers=number_threads) as executor:
            for obj in objects:
                try:
                    executor.submit(delete_object, s3_client, bucket_name, obj["Key"])
                    deleted = deleted + 1
                except Exception as e: # There is no asserting or raising exceptions, because it is meant to be used in a test
                    logging.error(f"Error deleting object {obj['Key']}: {e}")
        logging.info("All delete tasks have been submitted.")
    else:
        logging.info("No objects found in the bucket.")

    return deleted


### Fixtures

@pytest.fixture
def bucket_with_name(s3_client, request):
    # This fixtures automatically creates a bucket based on the name of the test that called it and then returns its name
    # Lastly, teardown the bucket by deleting it and its objects

    bucket_name = generate_valid_bucket_name(request.node.name.replace("_", "-"))
    create_bucket(s3_client, bucket_name)

    yield bucket_name

    delete_objects_multithreaded(s3_client,bucket_name)
    delete_bucket(s3_client, bucket_name)


import logging
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3_specs.docs.utils.utils import generate_valid_bucket_name, convert_unit
from s3_specs.docs.s3_helpers import generate_unique_bucket_name
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

import os
from tqdm import tqdm

### Functions


def create_bucket(s3_client, bucket_name, acl ='private'):
    """
    Create a new bucket on S3 ensuring that the location is set correctly.
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the to be created bucket
    :return: dict: response from boto3 create_bucket
    """

    # anything different than us-east-1 must have LocationConstraint on aws
    try:
        region = s3_client.meta.region_name
        if region != "us-east-1":
            return s3_client.create_bucket(
                ACL= acl,
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        logging.info(f"Bucket '{bucket_name}' already exists and is owned by you.")
    except s3_client.exceptions.BucketAlreadyExists:
        raise Exception(
            f"Bucket '{bucket_name}' already exists and is owned by someone else."
        )

    return s3_client.create_bucket(Bucket=bucket_name)


def upload_object(s3_client, bucket_name, object_key, body_file):
    """
    Create a new object on S3
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of bucket to upload the object
    :param object_key: str: key of the object
    :param body_file: file: file to be uploaded
    :return: HTTPStatusCode from boto3 put_object
    """

    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=body_file,
    )
    logging.info(f"Object {object_key} uploaded to bucket {bucket_name}")
    return response["ResponseMetadata"]["HTTPStatusCode"]


def upload_multiple_objects(
    s3_client, bucket_name, file_path: str, object_prefix: str, object_quantity: int
) -> int:
    """
    Utilizing multithreading uploads multiple objects while changing their names
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param file_path: str: list of paths of the objects to be uploaded
    :param object_prefix: str: prefix to be added to the object name
    :param object_quantity: int: number of objects to be uploaded
    :return: int: number of successful uploads
    """

    objects_names = [
        {"key": f"{object_prefix}-{i}", "path": file_path}
        for i in range(object_quantity)
    ]
    successful_uploads = upload_objects_multithreaded(
        s3_client, bucket_name, objects_names
    )

    return successful_uploads


def download_object(s3_client, bucket_name, object_key):
    """
    Download an object from a s3 Bucket
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param object_key: str: key of the object
    :return: HTTPStatusCode from boto3 get_object
    """

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Object {object_key} downloaded from bucket {bucket_name}")
        return response["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        logging.error(f"Error downloading object {object_key}: {e}")


# List all objects all at once as opossed to the regular list_objects_v2 which is limited to 1000 objects per request
def list_all_objects(s3_client, bucket_name):
    """
    List all objects in a bucket without size limits
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: list str: names of objects in the bucket
    """
    objects_names = []

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                objects_names.append(obj["Key"])

    return objects_names


def delete_object(s3_client, bucket_name, object_key):
    """
    Delete an object from a s3 Bucket
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param object_key: str: key of the object
    :return: HTTPStatusCode from boto3 delete_object
    """

    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Deleted object: {object_key} from bucket: {bucket_name}")
        return response["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        logging.error(f"Error deleting object {object_key}: {e}")


def delete_bucket(s3_client, bucket_name):
    """
    Delete a s3 bucket
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: dict: response from boto3 create_bucket
    """
    # Initializing to avoid erros when returning
    response = None

    try:
        response = s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")
    except s3_client.exceptions.NoSuchBucket:
        logging.error("No such Bucket")
    except Exception as e:
        logging.error(f"Error deleting bucket {bucket_name}: {e}")

    return response


# ## Multi-threading


def upload_objects_multithreaded(s3_client, bucket_name, objects_paths):
    """
    Upload all objects to one bucket in parallel
    The number of simultaneous uploads are limited by the number of objects in the list
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :param objects_paths: list: list of paths of the objects to be uploaded
    :return: int: number of successful uploads
    """

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # atributes processes to the available workers
        futures = [
            executor.submit(
                upload_object, s3_client, bucket_name, path["key"], path["path"]
            )
            for path in objects_paths
        ]

        # List all results of the futures
        successful_uploads = list(
            filter(lambda f: f.result() == 200, as_completed(futures))
        )
        logging.info(f"Successful uploads: {successful_uploads}")

        return len(successful_uploads)


def download_objects_multithreaded(s3_client, bucket_name):
    """
    Download all objects from a bucket in parallel
    The number of simultaneous downloads are limited by the number of objects in the list
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: int: number of successful downloads
    """

    objects_keys = list_all_objects(s3_client, bucket_name)

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # atributes processes to the available workers
        futures = [
            executor.submit(download_object, s3_client, bucket_name, key)
            for key in objects_keys
        ]

        # List all results of the futures
        successful_downloads = list(
            filter(lambda f: f.result() == 200, as_completed(futures))
        )
        logging.info(f"Successful downloads: {successful_downloads}")

    return len(successful_downloads)


def delete_objects_multithreaded(s3_client, bucket_name):
    """
    Delete all objects in a bucket in parallel
    :param s3_client: boto3 s3 client
    :param bucket_name: str: name of the bucket
    :return: int: number of successful deletions
    """
    objects_keys = list_all_objects(s3_client, bucket_name)

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # atributes processes to the available workers
        futures = [
            executor.submit(delete_object, s3_client, bucket_name, key)
            for key in objects_keys
        ]

        # List all results of the futures
        successful_deletions = list(
            filter(lambda f: f.result() == 204, as_completed(futures))
        )
        logging.info(f"Successful deletions: {successful_deletions}")

    return len(successful_deletions)


# ## Fixtures


@pytest.fixture
def fixture_bucket_with_name(s3_client, request):
    """
    This fixtures automatically creates a bucket based on the name of the test that called it and then returns its name
    Lastly, teardown the bucket by deleting it and its objects
    
    Creates a bucket with a random name and then tear it down
    :param s3_client: boto s3 cliet
    :param request: dict: contains the name of the current test and [optional] acl name
    :yield: str: generated bucket name    
    """

    # Setting up possible acl argument
    try:
        acl = request.param.get('acl')
    except:
        acl = 'private'

    # request.node get the name of the test currently running
    bucket_name = generate_valid_bucket_name(request.node.name.replace("_", "-"))
    create_bucket(s3_client, bucket_name, acl)

    yield bucket_name

    delete_objects_multithreaded(s3_client, bucket_name)
    delete_bucket(s3_client, bucket_name)

@pytest.fixture(params=['Enabled', 'Suspended'])
def fixture_versioned_bucket(s3_client,request):
    """
    Pytest fixture that creates an S3 bucket with versioning configuration.
    s3_client: Authenticated boto3 S3 client
    request: Pytest request object for parameter handling
    
    Yields: str: Name of the created bucket
    """
    try:
        # Get ACL from parameter or default to 'private'
        acl = getattr(request.param, 'acl', 'private')
        
        # Determine versioning status from parameter
        version_status = request.param if isinstance(request.param, str) else 'Enabled'
        if version_status not in ['Enabled', 'Suspended']:
            raise ValueError(f"Invalid version status: {version_status}")
        
        # Generate unique bucket name from test name
        bucket_name = generate_valid_bucket_name(request.node.name.replace("_", "-"))
        
        # Create bucket and configure versioning
        create_bucket(s3_client, bucket_name, acl)
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': version_status}
        )
        
        yield bucket_name
        
    except Exception as e:
        pytest.fail(f"Fixture setup failed: {str(e)}")
        
    finally:
        # Cleanup - runs whether test passes or fails
        try:
            delete_objects_multithreaded(s3_client, bucket_name)
            delete_bucket(s3_client, bucket_name)
        except Exception as cleanup_error:
            pytest.fail(f"Fixture cleanup failed: {str(cleanup_error)}")

@pytest.fixture
def fixture_upload_multiple_objects(s3_client, fixture_bucket_with_name, request):
    """
    Utilizing multithreading Fixture uploads multiple objects while changing their names
    :param s3_client: boto3 s3 client
    :param fixture_bucket_with_name: str: name of the bucket
    :param request: dict: "quantity" and "path"
     :return: int: number of successful uploads
    """

    qnt = request.param.get("quantity")
    path = request.param.get("path")

    logging.info(f"{qnt}, {path}")

    objects_names = [{"key": f"multiple-object'-{i}", "path": path} for i in range(qnt)]
    return upload_objects_multithreaded(
        s3_client, fixture_bucket_with_name, objects_names
    )


@pytest.fixture
def fixture_upload_multipart_file(s3_client, fixture_bucket_with_name, request) -> int:
    """
    Uploads a big file into multiple chunks to s3 bucket
    :param s3_client: boto3 s3 client
    :param fixture_bucket_with_name: pytest.fixture which setup and tears down bucket
    :param request: dict: contains file_path, file_size and object_key
    :return int: size in bytes of the obejct
    """
    bucket_name = fixture_bucket_with_name
    file_path = request.param.get("file_path")
    file_size = convert_unit(request.param.get("file_size"))
    object_key = request.param.get("object_key")

    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,  # Minimum size to start multipart upload
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True,
    )

    # Upload Progress Bar with time stamp
    with tqdm(
        total=file_size,
        desc=bucket_name,
        bar_format="Upload| {percentage:.1f}%|{bar:25}| {rate_fmt} | Time: {elapsed} | {desc}",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as pbar:
        response = s3_client.upload_file(
            file_path, bucket_name, object_key, Config=config, Callback=pbar.update
        )
        elapsed = pbar.format_dict["elapsed"]

        # Checking if the object was uploaded
        object_size = s3_client.get_object(Bucket=bucket_name, Key=object_key).get(
            "ContentLength", 0
        )

    return object_size, response, elapsed  # return int of size in bytes

@pytest.fixture(params=[{"prefix": "test-multiple-buckets-", "names": ["1", "2", "3"]}])
def fixture_multiple_buckets(request, s3_client):
    prefix = request.param.get("prefix")
    names = request.param.get("names")
    bucket_names = []
    for name in names:
        bucket_name = generate_unique_bucket_name(base_name=f"{prefix}{name}")
        create_bucket(s3_client, bucket_name)
        bucket_names.append(bucket_name)

    yield bucket_names

    for bucket_name in bucket_names:
        delete_bucket(s3_client, bucket_name)

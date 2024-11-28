import os
import boto3
import pytest
import time
import yaml
import logging
import subprocess
from s3_helpers import (
    generate_unique_bucket_name,
    delete_bucket_and_wait,
    create_bucket_and_wait,
    delete_object_and_wait,
    put_object_and_wait,
    cleanup_old_buckets,
    get_spec_path,
)
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


def pytest_addoption(parser):
    parser.addoption("--config", action="store", help="Path to the YAML config file")

@pytest.fixture
def test_params(request):
    """
    Loads test parameters from a config file or environment variable.
    """
    config_path = request.config.getoption("--config") or os.environ.get("CONFIG_PATH", "../params.example.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

@pytest.fixture
def default_profile(test_params):
    """
    Returns the default profile from test parameters.
    """
    return test_params["profiles"][test_params.get("default_profile_index", 0)]

@pytest.fixture
def lock_mode(default_profile):
    return default_profile["lock_mode"]

@pytest.fixture
def profile_name(default_profile):
    return (
        default_profile.get("profile_name")
        if default_profile.get("profile_name")
        else pytest.skip("This test requires a profile name")
    )

@pytest.fixture
def mgc_path(default_profile):
    """
    Validates and returns the path to the 'mgc' binary.
    """
    spec_dir = os.path.dirname(get_spec_path())
    path = os.path.join(spec_dir, default_profile.get("mgc_path", "mgc"))
    if not os.path.isfile(path):
        pytest.fail(f"The specified mgc_path '{path}' (absolute: {os.path.abspath(path)}) does not exist or is not a file.")
    return path

@pytest.fixture
def active_mgc_workspace(profile_name, mgc_path):
    # set the profile
    result = subprocess.run([mgc_path, "workspace", "set", profile_name],
                            capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("This test requires an mgc profile name")

    logging.info(f"mcg workspace set stdout: {result.stdout}")
    return profile_name

@pytest.fixture
def s3_client(default_profile):
    """
    Creates a boto3 S3 client using profile credentials or explicit config.
    """
    if "profile_name" in default_profile:
        session = boto3.Session(profile_name=default_profile["profile_name"])
    else:
        session = boto3.Session(
            region_name=default_profile["region_name"],
            aws_access_key_id=default_profile["aws_access_key_id"],
            aws_secret_access_key=default_profile["aws_secret_access_key"],
        )
    return session.client("s3", endpoint_url=default_profile.get("endpoint_url"))

@pytest.fixture
def bucket_name(request, s3_client):
    test_name = request.node.name.replace("_", "-")
    unique_name = generate_unique_bucket_name(base_name=f"{test_name}")

    # Yield the bucket name for the test to use
    yield unique_name

    # Teardown: delete the bucket after the test
    delete_bucket_and_wait(s3_client, unique_name)

@pytest.fixture
def existing_bucket_name(s3_client):
    # Generate a unique name for the bucket to simulate an existing bucket
    bucket_name = generate_unique_bucket_name(base_name="existing-bucket")

    # Ensure the bucket exists, creating it if necessary
    create_bucket_and_wait(s3_client, bucket_name)

    # Yield the existing bucket name to the test
    yield bucket_name

    # Teardown: delete the bucket after the test
    delete_bucket_and_wait(s3_client, bucket_name)

@pytest.fixture
def bucket_with_one_object(s3_client):
    # Generate a unique bucket name and ensure it exists
    bucket_name = generate_unique_bucket_name(base_name="fixture-bucket")
    create_bucket_and_wait(s3_client, bucket_name)

    # Define the object key and content, then upload the object
    object_key = "test-object.txt"
    content = b"Sample content for testing presigned URLs."
    put_object_and_wait(s3_client, bucket_name, object_key, content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Delete the object and bucket after the test
    delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)

@pytest.fixture
def versioned_bucket_with_one_object(s3_client, lock_mode):
    """
    Fixture to create a versioned bucket with one object for testing.
    
    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode for the bucket or objects (e.g., 'GOVERNANCE', 'COMPLIANCE')
    :return: Tuple containing bucket name, object key, and object version ID
    """
    base_name = "versioned-bucket-with-one-object"
    bucket_name = generate_unique_bucket_name(base_name=base_name)

    # Create bucket and enable versioning
    create_bucket_and_wait(s3_client, bucket_name)
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )

    # Upload a single object and get it's version
    object_key = "test-object.txt"
    content = b"Sample content for testing versioned object."
    object_version = put_object_and_wait(s3_client, bucket_name, object_key, content)

    # Yield details to tests
    yield bucket_name, object_key, object_version

    # Cleanup
    try:
        cleanup_old_buckets(s3_client, base_name, lock_mode)
    except Exception as e:
        print(f"Cleanup error {e}")

@pytest.fixture
def bucket_with_one_object_and_lock_enabled(s3_client, lock_mode, versioned_bucket_with_one_object):
    bucket_name, object_key, object_version = versioned_bucket_with_one_object
    # Enable bucket lock configuration if not already set
    s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            'ObjectLockEnabled': 'Enabled',
        }
    )
    logging.info(f"Object lock configuration enabled for bucket: {bucket_name}")

    # Yield details to tests
    yield bucket_name, object_key, object_version


@pytest.fixture
def lockeable_bucket_name(s3_client, lock_mode):
    """
    Fixture to create a versioned bucket for tests that will set default bucket object-lock configurations.

    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode ('GOVERNANCE', 'COMPLIANCE', or None)
    :return: The name of the created bucket
    """
    base_name = "lockeable-bucket"

    # Generate a unique name and create a versioned bucket
    bucket_name = generate_unique_bucket_name(base_name=base_name)
    create_bucket_and_wait(s3_client, bucket_name)
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )

    logging.info(f"Created versioned bucket: {bucket_name}")

    # Yield the bucket name for tests
    yield bucket_name

    # Cleanup after tests
    try:
        cleanup_old_buckets(s3_client, base_name, lock_mode)
    except Exception as e:
        logging.error(f"Cleanup error for bucket '{bucket_name}': {e}")

@pytest.fixture
def bucket_with_lock(lockeable_bucket_name, s3_client, lock_mode):
    """
    Fixture to create a bucket with Object Lock and a default retention configuration.

    :param lockeable_bucket_name: Name of the lockable bucket.
    :param s3_client: Boto3 S3 client.
    :param lock_mode: Lock mode ('GOVERNANCE' or 'COMPLIANCE').
    :return: The name of the bucket with Object Lock enabled.
    """
    bucket_name = lockeable_bucket_name

    # Enable Object Lock configuration with a default retention rule
    retention_days = 1
    s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": lock_mode,
                    "Days": retention_days
                }
            }
        }
    )

    logging.info(f"Bucket '{bucket_name}' configured with Object Lock and default retention.")

    return bucket_name

@pytest.fixture
def bucket_with_lock_and_object(s3_client, bucket_with_lock):
    """
    Prepares an S3 bucket with object locking enabled and uploads a dynamically
    generated object with versioning.

    :param s3_client: boto3 S3 client fixture.
    :param bucket_with_lock: Name of the bucket with versioning and object locking enabled.
    :return: Tuple of (bucket_name, object_key, object_version).
    """
    bucket_name = bucket_with_lock
    object_key = "test-object.txt"
    object_content = "This is a dynamically generated object for testing."

    # Upload the generated object to the bucket
    response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=object_content)
    object_version = response.get("VersionId")

    # Verify that the object is uploaded and has a version ID
    if not object_version:
        pytest.fail("Uploaded object does not have a version ID")

    # Return bucket name, object key, and version ID
    return bucket_name, object_key, object_version

@pytest.fixture
def bucket_with_one_object_acl(s3_client, bucket_with_one_object, request):
    """
    Prepares an S3 bucket with object and defines its obejct acl.

    :param s3_client: boto3 S3 client fixture.
    :param bucket_with_one_object: Name of the bucket with versioning and object locking enabled.
    :return: Tuple of (bucket_name, object_key, object_version).
    """
    
    bucket_name = "object_acl"
    #Create one bucket with an object
    bucket_name, object_key, url = bucket_with_one_object
    
    s3_client.put_object_acl(Bucket=bucket_name, ACL = request.param, Key= object_key)
    
    # Yield the bucket name and object key to the test
    yield bucket_name, object_key, url
import os
import boto3
import botocore
import pytest
import time
import yaml
import logging
from s3_helpers import (
    generate_unique_bucket_name,
    delete_bucket_and_wait,
    create_bucket_and_wait,
    delete_object_and_wait,
    put_object_and_wait,
    cleanup_old_buckets,
)

def pytest_addoption(parser):
    parser.addoption("--config", action="store", help="Path to the YAML config file")

@pytest.fixture
def test_params(request):
    # Check for --config parameter from pytest
    config_path = request.config.getoption("--config")
    if not config_path:
        # Fallback to papermill parameter if --config isn't provided
        config_path = os.environ.get("CONFIG_PATH", "params.yaml")

    with open(config_path, "r") as f:
        params = yaml.safe_load(f)
    return params

@pytest.fixture
def default_profile(test_params):
    default_profile_index = test_params.get("default_profile_index", 0)
    return test_params["profiles"][default_profile_index]

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
def s3_client(default_profile):

    # config can have just a profile name and it will use an existing .aws/config and .aws/credentials
    profile_name = default_profile.get("profile_name", None)
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
        return session.client("s3")

    # or it can have endpoint, region and credentials on the config instead
    region_name = default_profile.get("region_name")
    aws_access_key_id = default_profile.get("aws_access_key_id")
    aws_secret_access_key = default_profile.get("aws_secret_access_key")
    session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    endpoint_url = default_profile.get("endpoint_url")
    return session.client("s3", endpoint_url=endpoint_url)

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

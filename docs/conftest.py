import boto3
import pytest
import logging
from s3_helpers import generate_unique_bucket_name, delete_bucket_and_wait, create_bucket_and_wait, delete_object_and_wait

def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="default", help="AWS profile name")

@pytest.fixture
def s3_client(request):
    profile_name = request.config.getoption("--profile")
    session = boto3.Session(profile_name=profile_name)
    return session.client("s3")

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
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Delete the object and bucket after the test
    delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)


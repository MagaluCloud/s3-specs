import boto3
import pytest
from s3_helpers import generate_unique_bucket_name, delete_bucket_and_wait, create_bucket_and_wait

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
    unique_name = generate_unique_bucket_name(base_name=f"test-{test_name}")

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


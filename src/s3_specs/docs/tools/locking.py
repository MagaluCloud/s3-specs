import pytest
from uuid import uuid4
from s3_specs.docs.tools.utils import generate_valid_bucket_name
from s3_specs.docs.tools.crud import upload_object 
from s3_specs.docs.tools.versioning import fixture_versioned_bucket
from s3_specs.docs.s3_helpers import cleanup_old_buckets
from datetime import datetime, timedelta, timezone
from time import sleep

@pytest.fixture
def bucket_with_lock_enabled(s3_client, request):
    """
    Fixture to create a bucket with object lock enabled and clean up after the test.

    Args:
        s3_client: Boto3 S3 client fixture.
        fixture_versioned_bucket: Fixture providing a versioned bucket.

    Yields:
        str: The name of the bucket with object lock enabled.
    """
    bucket_name = generate_valid_bucket_name(request.node.name.replace("_", "-"))

    try:
        # Enable object lock on the bucket
        create_params = {
            "Bucket": bucket_name,
            "ObjectLockEnabledForBucket": True
        }

        response = s3_client.create_bucket(**create_params)

        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
        assert status_code == 200, f"{status_code} should be 200"

        lock_conf = s3_client.get_object_lock_configuration(Bucket=bucket_name)
        assert "ObjectLockConfiguration" in lock_conf

    except s3_client.exceptions.ClientError as e:
        pytest.fail(f"Failed to enable object lock on bucket {bucket_name}: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error enabling object lock on bucket {bucket_name}: {e}")

    yield bucket_name

    # Note: Locked buckets will be cleaned up by a separate process due to retention policies.
@pytest.fixture
def fixture_bucket_with_one_object_with_lock(s3_client, bucket_with_lock_enabled, fixture_create_small_file):
    """
    Fixture to create a versioned S3 bucket with a single object and locking enabled and then tear it down after 10 seconds

    Args:
        s3_client: A fixture containing the S3 client used to interact with the S3 service.
        fixture_versioned_bucket (str): A fixture that provides a versioned S3 bucket.
        fixture_create_small_file (Path): A fixture that creates a small file for testing.

    Yields:
        tuple: A tuple containing:
            - bucket_name (str): The name of the versioned S3 bucket.
            - object_key (str): The key of the uploaded object in the bucket.
            - source_path (Path): The local file path of the uploaded object.
    """
    object_key = f"versioned_object_{uuid4().hex}"
    source_path = fixture_create_small_file
    bucket_name = bucket_with_lock_enabled

    try:
        # Upload the object to the bucket
        upload_object(
            s3_client,
            bucket_name=bucket_name,
            object_key=object_key,
            body_file=str(source_path)
        )

        retain_until_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")  # converting to ISO 8601 format
        s3_client.put_object_retention(
            Bucket=bucket_name,
            Key=object_key,
            Retention={
                "Mode": "COMPLIANCE",  # Lock mode
                "RetainUntilDate": retain_until_date
            }
        )
    except Exception as e:
        pytest.fail(f"Unexpected error applying object lock on object {object_key}: {e}")

    # Verify if the object lock was applied correctly
    response = s3_client.get_object_retention(Bucket=bucket_name, Key=object_key)
    retention_mode = response.get("Retention", {}).get("Mode", None)
    if retention_mode != "COMPLIANCE":
        pytest.fail(f"Object lock is not correctly applied on object {object_key}")

    yield bucket_name, object_key, source_path
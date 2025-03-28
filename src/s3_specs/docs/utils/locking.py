import pytest
import logging
from s3_specs.docs.utils.utils import generate_valid_bucket_name
from s3_specs.docs.s3_helpers import cleanup_old_buckets

@pytest.fixture
def bucket_with_lock_enabled(s3_client, request):
    # use test name as base name for the bucket
    bucket_name = generate_valid_bucket_name(request.node.name)
    response = s3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    yield bucket_name

    try:
        cleanup_old_buckets(s3_client, base_name, lock_mode)
    except Exception as e:
        print(f"Cleanup error {e}")

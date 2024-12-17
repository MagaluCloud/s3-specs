# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Unique Bucket Names (create bucket)
#
# Buckets can be created with the **create_bucket** method on **boto3**, and are expected to fail with a `BucketAlreadyExists` error if there's already another bucket created, by any user of the system on any region, with the same name. The bucket names namespace is global.


# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import pytest
import botocore
import logging
import os
from s3_helpers import run_example, create_bucket

pytestmark = pytest.mark.basic
config = os.getenv("CONFIG", config)
# -


# ## Examples

# ### Create bucket
#
# Attempt to create the bucket - Expect Success


# +
def test_create_bucket(s3_client, bucket_name):
    # Ensure the bucket does not exist
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        pass

    logging.info(f"Bucket creation initiated.")
    response = create_bucket(s3_client, bucket_name)
    assert response.get("Location"), "Bucket location should be returned upon creation."
    logging.info(f'Created with Location: {response.get("Location")}')

    # Use waiter to confirm the bucket exists
    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' confirmed as created.")

run_example(__name__, "test_create_bucket", config=config)
# -


# ### Create the same bucket
# Attempt to create the same bucket again - Expect failure

# +
def test_create_same_bucket(s3_client, existing_bucket_name):
    logging.info(existing_bucket_name)

    if s3_client.meta.region_name == "us-east-1":
        response = create_bucket(s3_client, existing_bucket_name)
        assert response, "Create bucket with the same name on AWS on region US East (N. Virginia) should succeed"
        return

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        response = create_bucket(s3_client, existing_bucket_name)

    # Verify AccessDenied for the newly uploaded locked object
    error_code = exc_info.value.response['Error']['Code']
    # MagaluClod may return BucketAlreadyExists
    assert error_code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"], f"Expected BucketAlreadyOwnedByYou, got {error_code}"
    logging.info(f"Bucket '{existing_bucket_name}' already exists, as expected.")

run_example(__name__, "test_create_same_bucket", config=config)
# -


# ## References
#
# - [Boto3 Documentation: create_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html)
# - [Boto3 Documentation: Error Handling](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html)

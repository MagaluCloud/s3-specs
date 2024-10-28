# Unique Bucket Names (create bucket)

Buckets can be created with the **create_bucket** method on **boto3**, and are expected to fail with a `BucketAlreadyExists` error if there's already another bucket created, by any user of the system on any region, with the same name. The bucket names namespace is global.


```python
config = "../params/br-ne1.yaml"
docs_dir = "."
```


```python
# Parameters
config = "params/br-se1.yaml"
docs_dir = "docs"

```


```python
import pytest
import botocore
import logging
from s3_helpers import run_example, create_bucket
```

## Examples

### Create bucket

Attempt to create the bucket - Expect Success


```python
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

run_example(__name__, "unique-bucket-name", "test_create_bucket", config=config, docs_dir=docs_dir)
```

    
    docs/unique-bucket-name_test.py::test_create_bucket 

    
    -------------------------------- live log setup --------------------------------
    INFO     botocore.credentials:credentials.py:1278 Found credentials in shared credentials file: ~/.aws/credentials


    INFO     botocore.configprovider:configprovider.py:974 Found endpoint for s3 via: config_global.


    -------------------------------- live log call ---------------------------------
    INFO     root:unique-bucket-name_test.py:43 Bucket creation initiated.


    INFO     root:unique-bucket-name_test.py:46 Created with Location: /test-create-bucket-8eccf7


    INFO     root:unique-bucket-name_test.py:51 Bucket 'test-create-bucket-8eccf7' confirmed as created.


    PASSED

    
    ------------------------------ live log teardown -------------------------------
    INFO     root:s3_helpers.py:36 Bucket 'test-create-bucket-8eccf7' confirmed as deleted.


    
    
    ============================== 1 passed in 2.84s ===============================


### Create the same bucket
Attempt to create the same bucket again - Expect failure


```python
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

run_example(__name__, "unique-bucket-name", "test_create_same_bucket", config=config, docs_dir=docs_dir)
```

    
    docs/unique-bucket-name_test.py::test_create_same_bucket 

    
    -------------------------------- live log setup --------------------------------
    INFO     botocore.credentials:credentials.py:1278 Found credentials in shared credentials file: ~/.aws/credentials


    INFO     botocore.configprovider:configprovider.py:974 Found endpoint for s3 via: config_global.


    INFO     root:s3_helpers.py:58 Bucket 'existing-bucket-15d667' confirmed as created.


    -------------------------------- live log call ---------------------------------
    INFO     root:unique-bucket-name_test.py:62 existing-bucket-15d667


    INFO     root:unique-bucket-name_test.py:76 Bucket 'existing-bucket-15d667' already exists, as expected.


    PASSED

    
    ------------------------------ live log teardown -------------------------------
    INFO     root:s3_helpers.py:36 Bucket 'existing-bucket-15d667' confirmed as deleted.


    
    
    ============================== 1 passed in 2.05s ===============================


## References

- [Boto3 Documentation: create_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html)
- [Boto3 Documentation: Error Handling](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html)

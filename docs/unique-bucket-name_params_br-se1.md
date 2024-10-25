# Unique Bucket Names (create bucket)

Buckets can be created with the **create_bucket** method on **boto3**, and are expected to fail with a `BucketAlreadyExists` error if there's already another bucket created, by any user of the system on any region, with the same name. The bucket names namespace is global.

## Setup


```python
# Parameters
profile_name = "default"
```


```python
# Parameters
profile_name = "br-se1"

```


```python
# Import shared functions
from s3_helpers import print_timestamp, create_s3_client, generate_unique_bucket_name

print_timestamp()

# Create S3 client
s3_client = create_s3_client(profile_name)

# Generate a unique bucket name
bucket_name = generate_unique_bucket_name(base_name="test-unique-bucket-name")
print(f'test bucket will be named {bucket_name}')

```

    execution started at 2024-10-25 12:05:07.372537
    test bucket will be named test-unique-bucket-name-2ed5fa


## Examples

### Create bucket

Attempt to create the bucket - Expect Success


```python
# Delete it if it exists and it's yours
try:
    s3_client.delete_bucket(Bucket=bucket_name)
except s3_client.exceptions.NoSuchBucket:
    pass
    
print(f"Bucket creation initiated.")
response = s3_client.create_bucket(Bucket=bucket_name)
print(f'Created with Location: {response.get("Location")}')

# Use waiter to confirm the bucket exists
waiter = s3_client.get_waiter('bucket_exists')
waiter.wait(Bucket=bucket_name)
print(f"Bucket '{bucket_name}' confirmed as created.")
```

    Bucket creation initiated.


    Created with Location: /test-unique-bucket-name-2ed5fa
    Bucket 'test-unique-bucket-name-2ed5fa' confirmed as created.


### Create the same bucket
Attempt to create the same bucket again - Expect failure


```python
try:
    s3_client.create_bucket(Bucket=bucket_name)
    # If no error is raised, fail the test
    raise AssertionError("Expected BucketAlreadyExists error, but bucket was created successfully.")
except s3_client.exceptions.BucketAlreadyExists:
    print(f"Bucket '{bucket_name}' already exists, as expected.")
```

    Bucket 'test-unique-bucket-name-2ed5fa' already exists, as expected.


## Teardown


```python
try:
    s3_client.delete_bucket(Bucket=bucket_name)
except s3_client.exceptions.NoSuchBucket:
    print("already deleted by someone else")
    pass

# Use waiter to confirm the bucket doesnt exists
waiter = s3_client.get_waiter('bucket_not_exists')
waiter.wait(Bucket=bucket_name)
print(f"Bucket '{bucket_name}' confirmed as deleted.")
```

    Bucket 'test-unique-bucket-name-2ed5fa' confirmed as deleted.


## References

- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
- https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html

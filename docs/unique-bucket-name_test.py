# ---
# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # Unique Bucket Names (create bucket)
#
# Buckets can be created with the **create_bucket** method on **boto3**, and are expected to fail with a `BucketAlreadyExists` error if there's already another bucket created, by any user of the system on any region, with the same name. The bucket names namespace is global.


# + tags=["parameters"]
profile_name = "default"
# -


# + [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Setup


# +
import pytest
if __name__ == "__main__":
    from s3_helpers import print_timestamp, create_s3_client, generate_unique_bucket_name, delete_bucket_and_wait
    print_timestamp()
    s3_client = create_s3_client(profile_name)
    bucket_name = generate_unique_bucket_name(base_name="test-unique-bucket-name")
    print(f'test bucket will be named {bucket_name}')
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

    print(f"Bucket creation initiated.")
    response = s3_client.create_bucket(Bucket=bucket_name)
    assert response.get("Location"), "Bucket location should be returned upon creation."
    print(f'Created with Location: {response.get("Location")}')

    # Use waiter to confirm the bucket exists
    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' confirmed as created.")

if __name__ == "__main__":
    test_create_bucket(s3_client, bucket_name)
# -


# ### Create the same bucket
# Attempt to create the same bucket again - Expect failure


# +
def test_create_same_bucket(s3_client, existing_bucket_name):
    with pytest.raises(s3_client.exceptions.BucketAlreadyExists):
        s3_client.create_bucket(Bucket=existing_bucket_name)
    print(f"Bucket '{existing_bucket_name}' already exists, as expected.")

if __name__ == "__main__":
    test_create_same_bucket(s3_client, bucket_name)
# -


# + [markdown] jp-MarkdownHeadingCollapsed=true tags=["parameters"]
# ## Teardown


# +
if __name__ == "__main__":
    delete_bucket_and_wait(s3_client, bucket_name)
# -


# ## References
#
# - [Boto3 Documentation: create_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html)
# - [Boto3 Documentation: Error Handling](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html)

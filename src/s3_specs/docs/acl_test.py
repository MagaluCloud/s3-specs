# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---
import pytest
from itertools import product
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import update_existing_keys
import logging

pytestmark = pytest.mark.acl

# # OBJECT ACL

## Try to set object acl with invalid permissions
@pytest.mark.parametrize("acl_name", ['batata','""',]) 
def test_invalid_put_object_acl(s3_client, bucket_with_one_object, acl_name):
    bucket_name, object_key, _ = bucket_with_one_object
    try:
        s3_client.put_object_acl(Bucket = bucket_name, ACL = acl_name, Key = object_key)
        pytest.fail("Valid acl argument inputed, test failed")
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'


## Try to create obj acl with different permissions
@pytest.mark.parametrize("acl_name", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read',
]) 
def test_put_object_acl(s3_client, bucket_with_one_object, acl_name):
    bucket_name, object_key, _ = bucket_with_one_object
    response =  s3_client.put_object_acl(Bucket = bucket_name, ACL = acl_name, Key = object_key)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


## Test the creater profile always has FULL CONTROL of the objects with acl
@pytest.mark.parametrize("multiple_s3_clients, acl", [
    ({"number_clients": 1},'private'),
    ({"number_clients": 1},'public-read'),
    ({"number_clients": 1},'public-read-write'),
    ({"number_clients": 1},'authenticated-read')
], indirect=["multiple_s3_clients"]) 
def test_owner_get_object_acl(multiple_s3_clients, bucket_with_one_object, acl):

    s3_owner = multiple_s3_clients[0]
    bucket_name, obj_key, _ = bucket_with_one_object

    s3_owner.put_object_acl(Bucket = bucket_name, ACL = acl, Key=obj_key)


    bucket, key, _ = bucket_with_one_object
    response = s3_owner.get_object_acl(Bucket=bucket, Key=key)

    assert any([g['Permission'] == "FULL_CONTROL" for g in response['Grants']])

# # Bucket ACL


# ## Bucket ACL tests with 1 client

# test invalid arguments for acl
@pytest.mark.parametrize("acl_name", ["non-existing-acl-name", ""]) 
def test_invalid_put_bucket_acl(s3_client,existing_bucket_name, acl_name):
    try:
        s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
        pytest.fail("Valid acl argument inputed, test failed")
    except ClientError as e:
        assert e.response['ResponseMetadata']['HTTPStatusCode'] == 400

@pytest.mark.parametrize("acl_name", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read'
])
def test_valid_put_bucket_acl(s3_client, existing_bucket_name, acl_name):
    response = s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


# ## Bucket ACL tests with 2 authenticated clients (the owner and another user)

# Define bucket- and object-level operations
# see https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html
#
# Bucket-level:
# READ = s3:ListBucket, s3:ListBucketVersions, and s3:ListBucketMultipartUploads
# WRITE = s3:PutObject, s3:DeleteObjectVersion
bucket_read_operations = ['list_objects_v2', 'list_object_versions', 'list_multipart_uploads']
bucket_write_operations = ['put_object', 'delete_object']
#
# Object-level:
# READ = s3:GetObject, s3:GetObjectVersion
# WRITE = Not applicable
object_read_operations = ['get_object']
object_write_operations = []

bucket_operations = bucket_read_operations + bucket_write_operations

# Define what a second user is allowed to do for each ACL
non_owner_allowed_operations = {
    "public-read": {"bucket": bucket_read_operations, "object": object_read_operations},
    "authenticated-read": {"bucket": bucket_read_operations, "object": object_read_operations},
    "public-read-write": {
        "bucket": bucket_read_operations + bucket_write_operations,
        "object": object_read_operations + object_write_operations,
    },
    "bucket-owner-full-control": {"bucket": [], "object": []},
    "bucket-owner-read": {"bucket": [], "object": []},
    "log-delivery-write": {"bucket": [], "object": []},
    "private": {"bucket": [], "object": []},
}

# Define methods and their default inputs
methods_input = {
    'list_objects_v2': {"Bucket": ''},
    'list_object_versions': {"Bucket": ''},
    'list_multipart_uploads': {"Bucket": ''},
    # TODO: delete version when put object in prod start sending version id header
    # 'delete_object': {"Bucket": '', "Key": '', "VersionId": ''},
    'delete_object': {"Bucket": '', "Key": ''},
    'put_object': {"Bucket": '', "Key": '', "Body": 'test content'},
    'get_object': {"Bucket": '', "Key": ''},
}

# Generate test cases declaratively
test_cases = [
    (
        acl_name,
        method_name,
        [200, 204] if method_name in non_owner_allowed_operations[acl_name]["bucket"]
             or method_name in non_owner_allowed_operations[acl_name]["object"]
        else [403]
    )
    for acl_name, method_name in product(non_owner_allowed_operations.keys(), methods_input.keys())
]

# Generate descriptive test IDs
test_ids = [
    f"ACL={acl_name},Method={method_name},Context={'bucket' if method_name in bucket_operations else 'object'},Expected={expected_status_code}"
    for acl_name, method_name, expected_status_code in test_cases
]

# Test ACL permissions with 2 authenticated clients. This covers both bucket-level
# and object-level ACLs, verifying that the second client can only perform operations
# they are explicitly allowed to.
@pytest.mark.parametrize(
    "multiple_s3_clients, acl_name, method_name, expected_status_code",
    [({"number_clients": 2}, acl, method, status) for acl, method, status in test_cases],
    indirect=['multiple_s3_clients'],  # Indicate 'multiple_s3_clients' is a fixture
    ids=test_ids,  # Provide descriptive IDs for the test cases
)
def test_acl_operations(multiple_s3_clients, versioned_bucket_with_one_object, acl_name, method_name, expected_status_code):
    bucket_name, obj_key, obj_version = versioned_bucket_with_one_object

    # Set the bucket-level ACL
    s3_owner = multiple_s3_clients[0]
    s3_owner.put_bucket_acl(Bucket=bucket_name, ACL=acl_name)

    # Set the object-level ACL if required
    if method_name in non_owner_allowed_operations[acl_name]["object"]:
        s3_owner.put_object_acl(Bucket=bucket_name, Key=obj_key, ACL=acl_name)

    # Prepare arguments
    method_kwargs = update_existing_keys(
        methods_input[method_name],
        { "Bucket": bucket_name, "Key": obj_key, "ACL": acl_name, "VersionId": obj_version }
    )

    # Log prepared arguments and method name
    logging.info(f"method_args: {method_kwargs} method_name: {method_name}")

    # Execute the method
    s3_other = multiple_s3_clients[1]
    method = getattr(s3_other, method_name)

    try:
        response = method(**method_kwargs)
        actual_status_code = response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logging.info(f"error {e}")
        actual_status_code = e.response['ResponseMetadata']['HTTPStatusCode'] if hasattr(e, 'response') else 500

    # Assert the result
    assert actual_status_code in expected_status_code, (
        f"ACL: {acl_name}, Method: {method_name}, "
        f"Expected: {expected_status_code}, Actual: {actual_status_code}"
    )

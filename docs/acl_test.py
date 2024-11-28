import pytest
from botocore.exceptions import ClientError
import logging
from s3_helpers import(
    put_object_and_wait,
)
# #Bucket ACL

logging.basicConfig(format='%(message)s')


# ## test invalid arguments for acl
@pytest.mark.parametrize("acl_name", [
    "errado", "''"
]) 
def test_invalid_put_bucket_acl(s3_client,existing_bucket_name, acl_name):
    try:
        s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
        pytest.fail("Valid acl arg inputed, test failed")
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'


# ## Try to create acl with different permissions
@pytest.mark.parametrize("acl_name", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read',
]) 
def test_put_bucket_acl(s3_client, existing_bucket_name, acl_name):
    response =  s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


# ## Test the creater profile always has FULL CONTROL of the bucket
@pytest.mark.parametrize("acl_name", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read',
]) 
def test_get_bucket_acl(s3_client, existing_bucket_name,acl_name):
    s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
    response = s3_client.get_bucket_acl(Bucket=existing_bucket_name)
    
    assert any([g['Permission'] == "FULL_CONTROL" for g in response['Grants']])


# #OBJECT ACL

# ## test invalid arguments for acl
def test_invalid_put_object_acl(s3_client,existing_bucket_name, acl_name):
    try:
        s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
        pytest.fail("Valid acl arg inputed, test failed")
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'


# ## Try to set object acl with invalid permissions
@pytest.mark.parametrize("acl_name", ['batata','""',]) 
def test_invalid_put_object_acl(s3_client, bucket_with_one_object, acl_name):
    bucket_name, object_key, _ = bucket_with_one_object
    try:
        s3_client.put_object_acl(Bucket = bucket_name, ACL = acl_name, Key = object_key)
        pytest.fail("Valid acl argument inputed, test failed")
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'


# ## Try to create obj acl with different permissions
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


# ## Test the creater profile always has FULL CONTROL of the objects with acl
@pytest.mark.parametrize("bucket_with_one_object_acl", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read',
], indirect=True) 

def test_get_object_acl(s3_client, bucket_with_one_object_acl):

    bucket, key, _ = bucket_with_one_object_acl
    response = s3_client.get_object_acl(Bucket=bucket, Key=key)
    
    assert any([g['Permission'] == "FULL_CONTROL" for g in response['Grants']])









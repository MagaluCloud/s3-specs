import pytest
from botocore.exceptions import ClientError
from s3_helpers import (
    outer_merge
)

# # Bucket ACL

# ## Bucket ACL tests with 1 client
# test invalid arguments for acl
@pytest.mark.parametrize("acl_name, expected", [
    ("errado", 400),
    ( "''", 400)
]) 

def test_invalid_put_bucket_acl(s3_client,existing_bucket_name, acl_name, expected):
    try:
        s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
        pytest.fail("Valid acl argument inputed, test failed")
    except ClientError as e:
        assert e.response['ResponseMetadata']['HTTPStatusCode'] == expected


# Try to create acl with different permissions
@pytest.mark.parametrize("acl_name", [
    'private',
    'public-read',
    'public-read-write',
    'authenticated-read',
]) 

def test_put_bucket_acl(s3_client, existing_bucket_name, acl_name):
    response = s3_client.put_bucket_acl(Bucket = existing_bucket_name, ACL = acl_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


# ## Test ACL permissions with 2 clients

number_clients = 2

methods_input = {
    'list_objects_v2': {"Bucket": 'my-bucket'}, 
    'put_object': {"Bucket": 'my-bucket', "Key": 'my-key', "Body": 'content'},
    'get_object': {"Bucket": 'my-bucket', "Key": 'my-key'},
    'get_bucket_acl': {"Bucket": 'my-bucket'},
    #'put_bucket_acl': {"Bucket": 'my-bucket', "ACL": 'public-read'},
}

acl_permissions = ['private', 'public-read', 'public-read-write', 'authenticated-read', 'bucket-owner-read', 'bucket-owner-full-control', 'log-delivery-write']


expected_results = {
    'list_objects_v2': ['AccessDenied', 200, 200, 200, 'AccessDenied', 'AccessDenied', 'AccessDenied'],  
    'put_object': ['AccessDenied', 'AccessDenied', 200, 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],       
    'get_object': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],      
    'get_bucket_acl': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],   
    #'put_bucket_acl': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],   
}


@pytest.mark.parametrize(
    "multiple_s3_clients, acl_permission, method_args",
    [
        (
            {"number_clients": number_clients},  
            acl_permissions,  
            {
                "method": method,  
                "args": args, 
                "expected_results": expected_results[method]
            }
        )
        for method, args in methods_input.items()
    ],
    ids=list(methods_input.keys()),
    indirect=['multiple_s3_clients']  # Indicating that 'multiple_s3_clients' is a fixture
)

def test_bucket_acl(multiple_s3_clients, bucket_with_one_object, acl_permission, method_args):
    s3_owner = multiple_s3_clients[0]
    s3_other = multiple_s3_clients[1] 
    bucket_name, obj_key, _ = bucket_with_one_object
    
    results = []


    for acl in acl_permission:
        s3_owner.put_bucket_acl(Bucket = bucket_name, ACL = acl)
        #Get the arguments of inside of the subdict 
        
        input_method = outer_merge(method_args["args"], {'Bucket': bucket_name, 'Key': obj_key})

        method = getattr(s3_other, method_args["method"])
        
        try:
            response = method(**input_method)
            results.append(response['ResponseMetadata']['HTTPStatusCode'])
        except ClientError as e:
            results.append(e.response['Error']['Code'])

    assert results == method_args["expected_results"]
    
    
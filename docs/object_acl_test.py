import pytest
from botocore.exceptions import ClientError
from s3_helpers import (
    put_object_and_wait,
    outer_merge
)

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



# # Multiple clients tests

number_clients = 2

# Vars for the tests
methods_input = {
    'list_objects_v2': {"Bucket": 'my-bucket'}, 
    'put_object': {"Bucket": 'my-bucket', "Key": 'my-key', "Body": 'content'},
    'get_object': {"Bucket": 'my-bucket', "Key": 'my-key'},
    'get_object_acl': {"Bucket": 'my-bucket', "Key": 'my-key'},
}

acl_permissions = ['private', 'public-read', 'public-read-write', 'authenticated-read', 'bucket-owner-read', 'bucket-owner-full-control', 'log-delivery-write']


expected_results = {
    'list_objects_v2': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],  
    'put_object': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],       
    'get_object': ['AccessDenied', 200, 200, 200, 'AccessDenied', 'AccessDenied', 'AccessDenied'],      
    'get_object_acl': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],   
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


def test_object_acl(multiple_s3_clients, bucket_with_one_object, acl_permission, method_args):
    s3_owner = multiple_s3_clients[0]
    s3_guest= multiple_s3_clients[1] 
    bucket_name, obj_key, _ = bucket_with_one_object
    
    results = []

    for acl in acl_permission:
        s3_owner.put_object_acl(Bucket = bucket_name, ACL = acl, Key=obj_key)

        # Put the arguments of inside of the subdict for each method
        input_method = outer_merge(method_args["args"], {'Bucket': bucket_name, 'Key': obj_key})

        method = getattr(s3_guest, method_args["method"])
        
        try:
            response = method(**input_method)
            results.append(response['ResponseMetadata']['HTTPStatusCode'])
        except ClientError as e:
            results.append(e.response['Error']['Code'])
            

    assert results == method_args["expected_results"]
    



expected_results = {
    'list_objects_v2': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],  
    'put_object': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],       
    'get_object': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],      
    'get_object_acl': ['AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied', 'AccessDenied'],   
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
    
# ## Create 1 object with acl and anguestwithout acl and test the behavior of the functions
def test_2_clients_object_acl(multiple_s3_clients, bucket_with_one_object, acl_permission, method_args):
    s3_owner = multiple_s3_clients[0]
    s3_guest= multiple_s3_clients[1] 
    
    bucket_name, obj_key, _ = bucket_with_one_object
    obj_key_2 = obj_key + '2'
    content = 'this is the second object'

    put_object_and_wait(s3_owner, bucket_name, obj_key_2, content)
    
    results = []

    for acl in acl_permission:
        s3_owner.put_object_acl(Bucket = bucket_name, ACL = acl, Key = obj_key)
        #Get the arguments of inside of the subdict 
        
        input_method = outer_merge(method_args["args"], {'Bucket': bucket_name, 'Key': obj_key_2})

        method = getattr(s3_guest, method_args["method"])
        
        try:
            response = method(**input_method)
            results.append(response['ResponseMetadata']['HTTPStatusCode'])
        except ClientError as e:
            results.append(e.response['Error']['Code'])

    #Teardown
    s3_owner.delete_object(Bucket=bucket_name, Key=obj_key_2)


    assert results == method_args["expected_results"]
    

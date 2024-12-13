import pytest
from botocore.exceptions import ClientError

policy_dict_template = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "",
            "Principal": "",
            "Action": "",
            "Resource": ""
        }
    ]
}

# Example of the list for actions, tenants, and methods
actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
number_clients = 2
methods = ["get_object", "put_object", "delete_object"]
pytestmark = pytest.mark.policy

@pytest.mark.parametrize(
    'multiple_s3_clients, bucket_with_one_object_policy, boto3_action',
    [
        (
            {"number_clients": number_clients},
            {"policy_dict": policy_dict_template, "actions": action, "effect": "Deny"},
            method
        )
        for action, method in zip(actions, methods)
    ],
    indirect=['bucket_with_one_object_policy', 'multiple_s3_clients'],
)


def test_denied_policy_operations(multiple_s3_clients, bucket_with_one_object_policy, boto3_action):
    s3_clients_list = multiple_s3_clients
    
    bucket_name, object_key = bucket_with_one_object_policy

    kwargs = {
        'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        'Key': object_key
    }

    #PutObject needs another variable
    if boto3_action == 'put_object' :
        kwargs['Body'] = 'The answer for everthong is 42'
        
    #retrieve the method passed as argument
    
    try:
        method = getattr(s3_clients_list[1], boto3_action)
        method(**kwargs)
        pytest.fail("Expected exception not raised")
    except ClientError as e:
        assert e.response['Error']['Code'] == 'AccessDeniedByBucketPolicy'


expected = [200, 200, 204]

@pytest.mark.parametrize(
    'bucket_with_one_object_policy, multiple_s3_clients, boto3_action, expected',
    [
        (
            {"policy_dict": policy_dict_template, "actions": action, "effect": "Allow"},
            {"number_clients": number_clients},
            method,
            result,
        )
        for action, method, result in zip(actions, methods, expected)
    ],
    indirect=['bucket_with_one_object_policy', 'multiple_s3_clients'],
)


def test_allowed_policy_operations(multiple_s3_clients, bucket_with_one_object_policy, boto3_action, expected):
    s3_clients_list = multiple_s3_clients
    allowed_client = s3_clients_list[1] # not the bucket owner
    
    bucket_name, object_key = bucket_with_one_object_policy

    kwargs = {
        'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        'Key': object_key
    }

    #PutObject needs another variable
    if boto3_action == 'put_object' :
        kwargs['Body'] = 'The answer for everthong is 42'
        
    #retrieve the method passed as argument
    
    method = getattr(allowed_client, boto3_action)
    response = method(**kwargs)
    assert response['ResponseMetadata']['HTTPStatusCode'] == expected


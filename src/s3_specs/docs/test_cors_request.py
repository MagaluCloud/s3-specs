import os
import json
import pytest
import logging
import requests
from s3_specs.docs.s3_helpers import run_example, change_cors_json

pytestmark = [pytest.mark.homologacao, pytest.mark.cors]
config = "../params/br-ne1.yaml"
config = os.getenv("CONFIG", config)


def put_allow_all_policy_simple(s3_client, bucket_name):
    """
    Apply an 'Allow all' policy to the given bucket.
    """
    policy_dict = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [f"{bucket_name}", f"{bucket_name}/*"],
            }
        ],
    }
    policy_str = json.dumps(policy_dict)
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)
    logging.info(f"Policy 'Allow all' applied to bucket {bucket_name}")


base_cors_args = {
    "allowed_methods": ["GET", "PUT"],
    "allowed_origins": ["https://allowedorigin.com"],
    "allowed_headers": ["Authorization", "Content-Type"],
    "expose_headers": ["x-amz-request-id"],
    "max_age": 3600,
}

"""
Test cases: (method, origin, expected preflight status,
                expected real status, expect CORS headers)
"""
test_cases = [
    ("GET", "https://allowedorigin.com", 204, 200, True),       
    ("GET", "https://notallowed.com", 403, 200, False),         
    ("DELETE", "https://allowedorigin.com", 403, 403, False),   
    ("DELETE", "https://evil.com", 403, 403, False),            
    ("PUT", "https://allowedorigin.com", 204, 400, True),       
    ("PUT", "https://evil.com", 403, 400, False),
]


@pytest.mark.parametrize("test_case", test_cases)
def test_real_cors_request(s3_client, existing_bucket_name, test_case):
    """
    Test CORS preflight OPTIONS and actual requests for given HTTP methods and origins.
    """
    cors_args = base_cors_args
    method, origin, expected_preflight_status, expected_real_status, expect_cors = test_case

    put_allow_all_policy_simple(s3_client, existing_bucket_name)

    cors_config = change_cors_json(bucket_name=existing_bucket_name, cors_args=cors_args)
    s3_client.put_bucket_cors(Bucket=existing_bucket_name, CORSConfiguration=cors_config)
    result = s3_client.get_bucket_cors(Bucket=existing_bucket_name)
    applied_methods = result["CORSRules"][0]["AllowedMethods"]
    assert sorted(applied_methods) == sorted(cors_args["allowed_methods"])

    endpoint_url = s3_client.meta.endpoint_url
    bucket_url = f"{endpoint_url}/{existing_bucket_name}"

    # Preflight OPTIONS request headers
    preflight_headers = {
        "Origin": origin,
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": ",".join(cors_args["allowed_headers"]),
        "Authorization": "Fake-Token",
    }
    preflight_resp = requests.options(bucket_url, headers=preflight_headers)
    assert preflight_resp.status_code == expected_preflight_status
    if expect_cors:
        assert preflight_resp.headers.get("Access-Control-Allow-Origin") == origin
        assert method in preflight_resp.headers.get("Access-Control-Allow-Methods", "")
        for hdr in cors_args["allowed_headers"]:
            assert hdr.lower() in preflight_resp.headers.get("Access-Control-Allow-Headers", "").lower()
        assert preflight_resp.headers.get("Access-Control-Max-Age") == str(cors_args["max_age"])
    else:
        assert "Access-Control-Allow-Origin" not in preflight_resp.headers

    # Real request after preflight
    request_func = {
        "GET": requests.get,
        "PUT": requests.put,
        "DELETE": requests.delete,
        "POST": requests.post
    }

    if method not in request_func:
        raise ValueError(f"HTTP method '{method}' not supported in test")

    real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
    
    # Body for PUT requests (encoded as UTF-8 bytes)
    data = None
    if method in ("PUT"):
        data = "Test content".encode("utf-8")

    real_resp = request_func[method](bucket_url, headers=real_headers, data=data)

    assert real_resp.status_code == expected_real_status

    if expect_cors:
        assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
    else:
        assert "Access-Control-Allow-Origin" not in real_resp.headers


run_example(__name__, "test_real_cors_request", config=config)


def cleanup_bucket(s3_client, bucket_name):
    """
    Delete all objects in the bucket and then delete the bucket itself.
    """
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} successfully removed")
    except Exception as e:
        logging.warning(f"Error cleaning bucket {bucket_name}: {str(e)}")

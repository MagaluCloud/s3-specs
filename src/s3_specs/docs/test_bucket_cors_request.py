import os
import json
import pytest
import logging
import requests
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.permission import generate_policy

pytestmark = [pytest.mark.homologacao, pytest.mark.cors]
config = "../params/br-ne1.yaml"
config = os.getenv("CONFIG", config)

base_cors_args = {
    "AllowedMethods": ["GET", "PUT"],
    "AllowedOrigins": ["https://allowedorigin.com"],
    "AllowedHeaders": ["Authorization", "Content-Type"],
    "ExposeHeaders": ["x-amz-request-id"],
    "MaxAgeSeconds": 3600,
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
    method, origin, expected_preflight_status, expected_real_status, expect_cors = (
        test_case
    )

    policy_str = generate_policy(
        effect="Allow",
        principals="*",
        actions="s3:*",
        resources=[f"{existing_bucket_name}", f"{existing_bucket_name}/*"],
    )
    s3_client.put_bucket_policy(Bucket=existing_bucket_name, Policy=policy_str)
    logging.info(f"Policy 'Allow all' applied to bucket {existing_bucket_name}")

    cors_config = {"CORSRules": [cors_args]}
    s3_client.put_bucket_cors(
        Bucket=existing_bucket_name, CORSConfiguration=cors_config
    )
    result = s3_client.get_bucket_cors(Bucket=existing_bucket_name)
    applied_methods = result["CORSRules"][0]["AllowedMethods"]
    assert sorted(applied_methods) == sorted(cors_args["AllowedMethods"])

    endpoint_url = s3_client.meta.endpoint_url
    bucket_url = f"{endpoint_url}/{existing_bucket_name}"

    # Preflight OPTIONS request headers
    preflight_headers = {
        "Origin": origin,
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": ",".join(cors_args["AllowedHeaders"]),
        "Authorization": "Fake-Token",
    }
    preflight_resp = requests.options(bucket_url, headers=preflight_headers)
    assert preflight_resp.status_code == expected_preflight_status
    if expect_cors:
        assert preflight_resp.headers.get("Access-Control-Allow-Origin") == origin
        assert method in preflight_resp.headers.get("Access-Control-Allow-Methods", "")
        for hdr in cors_args["AllowedHeaders"]:
            assert (
                hdr.lower()
                in preflight_resp.headers.get(
                    "Access-Control-Allow-Headers", ""
                ).lower()
            )
        assert preflight_resp.headers.get("Access-Control-Max-Age") == str(
            cors_args["MaxAgeSeconds"]
        )
    else:
        assert "Access-Control-Allow-Origin" not in preflight_resp.headers

    # Real request after preflight
    request_func = {
        "GET": requests.get,
        "PUT": requests.put,
        "DELETE": requests.delete,
        "POST": requests.post,
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

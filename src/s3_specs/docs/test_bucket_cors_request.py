import uuid
import time
import pytest
import logging
import requests
from s3_specs.docs.s3_helpers import create_bucket_and_wait
from s3_specs.docs.tools.permission import generate_policy

pytestmark = [pytest.mark.homologacao, pytest.mark.cors]

BASE_CORS_CONFIG = {
    "standard": {
        "AllowedMethods": ["GET", "PUT", "POST", "OPTIONS"],
        "AllowedOrigins": ["https://allowedorigin.com", "https://sub.allowedorigin.com"],
        "AllowedHeaders": ["Authorization", "Content-Type", "X-Custom-Header"],
        "ExposeHeaders": ["x-amz-request-id"],
        "MaxAgeSeconds": 3600
    },
    "wildcard": {
        "AllowedMethods": ["GET", "HEAD"],
        "AllowedOrigins": ["*"],
        "AllowedHeaders": ["*"],
        "ExposeHeaders": [],
        "MaxAgeSeconds": 0
    },
    "restricted": {
        "AllowedMethods": ["GET"],
        "AllowedOrigins": ["https://strict.com"],
        "AllowedHeaders": ["Authorization"],
        "ExposeHeaders": [],
        "MaxAgeSeconds": 300
    }
}

PREFLIGHT_TEST_CASES = [
    # Standard config tests
    {
        "name": "standard_preflight_allowed_origin",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
        "name": "standard_preflight_subdomain",
        "config": "standard",
        "origin": "https://sub.allowedorigin.com",
        "method": "PUT",
        "headers": ["Content-Type"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
        "name": "standard_preflight_not_allowed_origin",
        "config": "standard",
        "origin": "https://evil.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "standard_preflight_not_allowed_method",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "DELETE",
        "headers": ["Authorization"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "standard_preflight_not_allowed_header",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "GET",
        "headers": ["Not-Allowed-Header"],
        "expected_status": 403,
        "expect_cors": False
    },
    
    # Wildcard config tests
    {
        "name": "wildcard_preflight_any_origin",
        "config": "wildcard",
        "origin": "https://anyorigin.com",
        "method": "GET",
        "headers": ["Any-Header"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
        "name": "wildcard_preflight_not_allowed_method",
        "config": "wildcard",
        "origin": "https://anyorigin.com",
        "method": "POST",
        "headers": ["Any-Header"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "wildcard_preflight_head_method",
        "config": "wildcard",
        "origin": "https://anyorigin.com",
        "method": "HEAD",
        "headers": ["Any-Header"],
        "expected_status": 204,
        "expect_cors": True
    },
    
    # Restricted config tests
    {
        "name": "restricted_preflight_exact_origin",
        "config": "restricted",
        "origin": "https://strict.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
        "name": "restricted_preflight_wrong_origin",
        "config": "restricted",
        "origin": "https://almost-strict.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "restricted_preflight_missing_header",
        "config": "restricted",
        "origin": "https://strict.com",
        "method": "GET",
        "headers": [],
        "expected_status": 403,
        "expect_cors": False,
        "required_headers": ["Authorization"]
    },
    
    # Additional test cases
    {
        "name": "standard_preflight_http_vs_https",
        "config": "standard",
        "origin": "http://allowedorigin.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "standard_preflight_different_port",
        "config": "standard",
        "origin": "https://allowedorigin.com:8080",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 403,
        "expect_cors": False
    },
    {
        "name": "standard_preflight_empty_origin",
        "config": "standard",
        "origin": "",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 405,
        "expect_cors": False
    },
    {
        "name": "standard_preflight_options_method",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "OPTIONS",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
    "name": "standard_preflight_no_acrh",
    "config": "standard",
    "origin": "https://allowedorigin.com",
    "method": "GET",
    "headers": [],
    "expected_status": 204,
    "expect_cors": True
    },
    {
    "name": "wildcard_allowedheaders_star_with_sensitive_headers",
    "config": "wildcard",
    "origin": "https://anyorigin.com",
    "method": "GET",
    "headers": ["authorization", "x-custom-sensitive-header"],
    "expected_status": 204,
    "expect_cors": True
    },
    {
    "name": "wildcard_allowedheaders_star_without_acrh",
    "config": "wildcard",
    "origin": "https://anyorigin.com",
    "method": "GET",
    "headers": None,
    "expected_status": 204,
    "expect_cors": True,
    "expected_cors_headers": {
        "Access-Control-Allow-Headers": "*"
    }
    },
    {
    "name": "wildcard_origin_subdomain",
    "config": "wildcard",
    "origin": "https://sub.allowedorigin.com",
    "method": "GET",
    "headers": ["Authorization"],
    "expected_status": 204,
    "expect_cors": True
    },
    {
    "name": "standard_preflight_acrh_spaces_and_case",
    "config": "standard",
    "origin": "https://allowedorigin.com",
    "method": "GET",
    "headers": ["Authorization", "X-Custom-Header"],
    "expected_status": 204,
    "expect_cors": True
    },
    {
    "name": "standard_preflight_method_options_allowed",
    "config": "standard",
    "origin": "https://allowedorigin.com",
    "method": "OPTIONS",
    "headers": [],
    "expected_status": 204,
    "expect_cors": True
    }, 
    {
    "name": "standard_preflight_header_case_insensitive",
    "config": "standard",
    "origin": "https://allowedorigin.com",
    "method": "GET",
    "headers": ["authorization"],
    "expected_status": 204,
    "expect_cors": True
    },
    {
        "name": "standard_preflight_multiple_headers",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "PUT",
        "headers": ["Content-Type", "Authorization"],
        "expected_status": 204,
        "expect_cors": True
    },
    {
        "name": "standard_preflight_exposed_headers",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True,
        "check_exposed_headers": True
    },
    {
        "name": "standard_preflight_max_age",
        "config": "standard",
        "origin": "https://allowedorigin.com",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True,
        "check_max_age": True
    },
    # Console test case
    {
        "name": "console_origin_test_case",
        "config": "standard",
        "origin": "https://console.magalu.cloud",
        "method": "GET",
        "headers": ["Authorization"],
        "expected_status": 204,
        "expect_cors": True,
        "check_max_age": True
    }
]

@pytest.fixture
def cors_buckets(s3_client):
    """Create buckets with different CORS configurations once for the module"""
    buckets = {}
    for config_name, config in BASE_CORS_CONFIG.items():
        bucket_name = f"cors-test-{config_name}-{uuid.uuid4().hex[:8]}"
        create_bucket_and_wait(s3_client, bucket_name)
        policy = generate_policy(
            effect="Allow",
            principals="*",
            actions="s3:*",
            resources=[bucket_name, f"{bucket_name}/*"],
        )
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
        s3_client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration={"CORSRules": [config]}
        )
        def check_cors():
            result = s3_client.get_bucket_cors(Bucket=bucket_name)
            assert sorted(result["CORSRules"][0]["AllowedMethods"]) == sorted(config["AllowedMethods"])
            return result
        execute_request_with_retry(check_cors)
        buckets[config_name] = bucket_name
    yield buckets
    for bucket_name in buckets.values():
        cleanup_bucket(s3_client, bucket_name)

def execute_request_with_retry(request_func, max_attempts=3, delay=1):
    """Execute with retry logic"""
    last_exception = None
    for attempt in range(max_attempts):
        try:
            return request_func()
        except (AssertionError, Exception) as e:
            last_exception = e
            if attempt < max_attempts - 1:
                logging.warning(f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}")
                time.sleep(delay)
            else:
                raise last_exception

def validate_preflight_response(resp, test_case, cors_config):
    """Validate preflight response against expectations"""
    if test_case["expect_cors"]:
        assert resp.status_code == test_case["expected_status"]
        assert resp.headers.get("Access-Control-Allow-Origin") == test_case["origin"], (
            f"Expected origin {test_case['origin']}, got {resp.headers.get('Access-Control-Allow-Origin')}"
        )
        
        allowed_methods = resp.headers.get("Access-Control-Allow-Methods", "").split(", ")
        assert test_case["method"] in allowed_methods, f"Method {test_case['method']} not in allowed methods"
        
        if cors_config["AllowedHeaders"] != ["*"]:
            allowed_hdrs = resp.headers.get("Access-Control-Allow-Headers", "").lower()
            for hdr in test_case["headers"]:
                assert hdr.lower() in allowed_hdrs, f"Header {hdr} not allowed"
        
        if test_case.get("check_max_age") and cors_config["MaxAgeSeconds"] > 0:
            assert "Access-Control-Max-Age" in resp.headers, "Max-Age header missing"
            assert resp.headers["Access-Control-Max-Age"] == str(cors_config["MaxAgeSeconds"])
        
        if test_case.get("check_exposed_headers"):
            exposed_headers = resp.headers.get("Access-Control-Expose-Headers", "").split(", ")
            for hdr in cors_config["ExposeHeaders"]:
                assert hdr in exposed_headers, f"Exposed header {hdr} missing"
    else:
        assert resp.status_code == test_case["expected_status"]
        assert "Access-Control-Allow-Origin" not in resp.headers, "Unexpected CORS headers"

def cleanup_bucket(s3_client, bucket_name):
    """Cleanup bucket helper"""
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        logging.warning(f"Cleanup failed for {bucket_name}: {str(e)}")

@pytest.mark.parametrize("test_case", PREFLIGHT_TEST_CASES, ids=[tc["name"] for tc in PREFLIGHT_TEST_CASES])
def test_preflight_scenarios(s3_client, cors_buckets, test_case):
    """Test various preflight scenarios against different CORS configurations"""
    bucket_name = cors_buckets[test_case["config"]]
    endpoint_url = s3_client.meta.endpoint_url
    bucket_url = f"{endpoint_url}/{bucket_name}"
    cors_config = BASE_CORS_CONFIG[test_case["config"]]
    def execute_test():
        headers = {
            "Origin": test_case["origin"],
            "Access-Control-Request-Method": test_case["method"],
        }
        if test_case["headers"] or test_case.get("required_headers"):
            headers["Access-Control-Request-Headers"] = ",".join(test_case["headers"])
        resp = requests.options(bucket_url, headers=headers)
        logging.info(f"Preflight test {test_case['name']} - Status: {resp.status_code}")
        logging.debug(f"Response headers: {resp.headers}")
        validate_preflight_response(resp, test_case, cors_config)
        return resp
    execute_request_with_retry(execute_test)

def test_multiple_cors_rules_preflight(s3_client):
    """Test bucket with multiple CORS rules (preflight only)"""
    bucket_name = f"cors-multi-{uuid.uuid4().hex[:8]}"
    create_bucket_and_wait(s3_client, bucket_name)
    try:
        multi_rules = {
            "CORSRules": [
                {
                    "AllowedMethods": ["GET"],
                    "AllowedOrigins": ["https://site1.com"],
                    "AllowedHeaders": ["Header1"],
                    "MaxAgeSeconds": 100
                },
                {
                    "AllowedMethods": ["PUT", "POST"],
                    "AllowedOrigins": ["https://site2.com"],
                    "AllowedHeaders": ["Header2"],
                    "ExposeHeaders": ["x-amz-meta"]
                }
            ]
        }
        s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=multi_rules)
        def check_rules():
            result = s3_client.get_bucket_cors(Bucket=bucket_name)
            assert len(result["CORSRules"]) == 2
            return result
        execute_request_with_retry(check_rules)
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        def test_first_rule():
            headers = {
                "Origin": "https://site1.com",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Header1"
            }
            resp = requests.options(bucket_url, headers=headers)
            assert resp.status_code == 204
            assert resp.headers.get("Access-Control-Allow-Origin") == "https://site1.com"
            return resp
        execute_request_with_retry(test_first_rule)
        def test_second_rule():
            headers = {
                "Origin": "https://site2.com",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "Header2"
            }
            resp = requests.options(bucket_url, headers=headers)
            assert resp.status_code == 204
            assert resp.headers.get("Access-Control-Allow-Origin") == "https://site2.com"
            return resp
        execute_request_with_retry(test_second_rule)
        def test_non_matching():
            headers = {
                "Origin": "https://site3.com",
                "Access-Control-Request-Method": "GET",
            }
            resp = requests.options(bucket_url, headers=headers)
            assert resp.status_code == 403
            return resp
        execute_request_with_retry(test_non_matching)
    finally:
        cleanup_bucket(s3_client, bucket_name)

def test_no_cors_configuration_preflight(s3_client):
    """Test bucket with no CORS configuration (preflight only)"""
    bucket_name = f"cors-none-{uuid.uuid4().hex[:8]}"
    create_bucket_and_wait(s3_client, bucket_name)
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        def execute_test():
            headers = {
                "Origin": "https://anyorigin.com",
                "Access-Control-Request-Method": "GET",
            }
            resp = requests.options(bucket_url, headers=headers)
            assert resp.status_code == 204
            assert "Access-Control-Allow-Origin" in resp.headers
            return resp
        execute_request_with_retry(execute_test)
    finally:
        cleanup_bucket(s3_client, bucket_name)
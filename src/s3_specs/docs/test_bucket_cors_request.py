import uuid
import time
import pytest
import logging
import requests
from s3_specs.docs.s3_helpers import create_bucket_and_wait
from s3_specs.docs.tools.permission import generate_policy

pytestmark = [pytest.mark.homologacao, pytest.mark.cors]

base_cors_args = {
    "AllowedMethods": ["GET", "PUT", "OPTIONS"],
    "AllowedOrigins": ["https://allowedorigin.com"],
    "AllowedHeaders": ["Authorization", "Content-Type"],
    "ExposeHeaders": ["x-amz-request-id"],
    "MaxAgeSeconds": 0
}


def execute_request_with_retry(request_func, max_attempts=3, delay=1):
    """
    Execute an HTTP request with retry logic for failed assertions
    
    Args:
        request_func: Function that executes the request and validations
        max_attempts: Maximum number of attempts
        delay: Delay between attempts in seconds
    """
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return request_func()
        except AssertionError as e:
            last_exception = e
            if attempt < max_attempts - 1:
                logging.warning(f"Request failed (attempt {attempt + 1}/{max_attempts}): {str(e)}")
                time.sleep(delay)
            else:
                logging.error(f"Request failed after {max_attempts} attempts")
                raise last_exception
        except Exception as e:
            last_exception = e
            if attempt < max_attempts - 1:
                logging.warning(f"Request error (attempt {attempt + 1}/{max_attempts}): {str(e)}")
                time.sleep(delay)
            else:
                logging.error(f"Request error after {max_attempts} attempts")
                raise last_exception


def setup_bucket_with_cors(s3_client):
    """Helper to create bucket with CORS configured"""
    bucket_name = f"cors-test-{uuid.uuid4().hex[:10]}"
    create_bucket_and_wait(s3_client, bucket_name)
    
    policy = generate_policy(
        effect="Allow",
        principals="*",
        actions="s3:*",
        resources=[bucket_name, f"{bucket_name}/*"],
    )
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
    
    s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": [base_cors_args]})
    # time.sleep(2)
    
    def check_cors():
        result = s3_client.get_bucket_cors(Bucket=bucket_name)
        print(f"CORS APPLIED == {result}")
        assert sorted(result["CORSRules"][0]["AllowedMethods"]) == sorted(base_cors_args["AllowedMethods"])
        return result
    
    execute_request_with_retry(check_cors, max_attempts=3, delay=1)
    return bucket_name


def validate_cors_headers(headers, origin, method, should_have_cors):
    """Helper to validate CORS headers"""
    if should_have_cors:
        assert headers.get("Access-Control-Allow-Origin") == origin, f"Expected origin {origin}, got {headers.get('Access-Control-Allow-Origin')}"
        assert method in headers.get("Access-Control-Allow-Methods", ""), f"Method {method} not in {headers.get('Access-Control-Allow-Methods')}"
        allowed_hdrs = headers.get("Access-Control-Allow-Headers", "").lower()
        if allowed_hdrs != "*":
            for hdr in base_cors_args["AllowedHeaders"]:
                assert hdr.lower() in allowed_hdrs, f"Header {hdr} not in {allowed_hdrs}"
    else:
        assert "Access-Control-Allow-Origin" not in headers, f"Unexpected CORS header found: {headers.get('Access-Control-Allow-Origin')}"


def cleanup_bucket(s3_client, bucket_name):
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} successfully removed")
    except Exception as e:
        logging.warning(f"Error cleaning bucket {bucket_name}: {str(e)}")


def test_get_allowed_origin(s3_client):
    """Test GET with allowed origin - with request retry"""
    method = "GET"
    origin = "https://allowedorigin.com"
    expect_cors = True
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            assert preflight_resp.status_code == 204
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            real_resp = requests.get(bucket_url, headers=real_headers)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)


def test_get_not_allowed_origin(s3_client):
    """Test GET with not allowed origin - with request retry"""
    method = "GET"
    origin = "https://notallowed.com"
    expect_cors = False
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            assert preflight_resp.status_code == 403
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            real_resp = requests.get(bucket_url, headers=real_headers)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)





def test_put_allowed_origin(s3_client):
    """Test PUT with allowed origin - with request retry"""
    method = "PUT"
    origin = "https://allowedorigin.com"
    expect_cors = True
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            assert preflight_resp.status_code == 204
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            data = "Test content".encode()
            real_resp = requests.put(bucket_url, headers=real_headers, data=data)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)


def test_put_not_allowed_origin(s3_client):
    """Test PUT with not allowed origin - with request retry"""
    method = "PUT"
    origin = "https://evil.com"
    expect_cors = False
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            assert preflight_resp.status_code == 403
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            data = "Test content".encode()
            real_resp = requests.put(bucket_url, headers=real_headers, data=data)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)

def test_delete_allowed_origin(s3_client):
    """Test DELETE with allowed origin (method not allowed) - with request retry"""
    method = "DELETE"
    origin = "https://allowedorigin.com"
    expect_cors = False
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            real_resp = requests.delete(bucket_url, headers=real_headers)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)


def test_delete_not_allowed_origin(s3_client):
    """Test DELETE with not allowed origin - with request retry"""
    method = "DELETE"
    origin = "https://evil.com"
    expect_cors = False
    
    bucket_name = setup_bucket_with_cors(s3_client)
    
    try:
        endpoint_url = s3_client.meta.endpoint_url
        bucket_url = f"{endpoint_url}/{bucket_name}"
        
        def execute_preflight():
            preflight_headers = {
                "Origin": origin,
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": ",".join(base_cors_args["AllowedHeaders"]),
            }
            preflight_resp = requests.options(bucket_url, headers=preflight_headers)
            print(f"\n[PREFLIGHT] {method=} {origin=}")
            print(f"PREFLIGHT RESP == {preflight_resp.status_code}")
            print(f"HEADERS: {preflight_resp.headers}")
            
            validate_cors_headers(preflight_resp.headers, origin, method, expect_cors)
            return preflight_resp
        
        execute_request_with_retry(execute_preflight, max_attempts=3, delay=1)
        
        def execute_real_request():
            real_headers = {"Origin": origin, "Authorization": "Fake-Token"}
            real_resp = requests.delete(bucket_url, headers=real_headers)
            print(f"\n[REAL REQ] {method=} {origin=}")
            print(f"REAL RESP == {real_resp.status_code}")
            print(f"HEADERS: {real_resp.headers}")
            
            if expect_cors:
                assert real_resp.headers.get("Access-Control-Allow-Origin") == origin
            else:
                if real_resp.status_code < 400:
                    assert "Access-Control-Allow-Origin" not in real_resp.headers
            return real_resp
        
        execute_request_with_retry(execute_real_request, max_attempts=3, delay=1)
    
    finally:
        cleanup_bucket(s3_client, bucket_name)
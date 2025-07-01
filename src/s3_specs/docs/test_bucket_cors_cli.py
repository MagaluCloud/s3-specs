import pytest
import json
import tempfile
import logging
from s3_specs.docs.tools.utils import execute_subprocess
from s3_specs.docs.s3_helpers import change_cors_json

pytestmark = [pytest.mark.homologacao, pytest.mark.cli, pytest.mark.cors]

cors_test_cases = [
    # ===== Basic Tests =====
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 1234,
            },
            "expect_fail": False
        },
        id="simple-valid-config"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET", "PUT"],
                "allowed_origins": ["https://allowed.com", "https://other.com"],
                "allowed_headers": ["Authorization", "Content-Type"],
                "expose_headers": [],
                "max_age": 3600,
            },
            "expect_fail": False
        },
        id="multiple-methods-and-origins"
    ),

    # ===== Wildcard Tests =====
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": [],
                "allowed_headers": ["*"],
                "expose_headers": [],
                "max_age": 300,
            },
            "expect_fail": True
        },
        id="wildcard-header-no-origins"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["*"],
                "allowed_headers": [],
                "expose_headers": [],
                "max_age": 0,
            },
            "expect_fail": False
        },
        id="wildcard-origin-empty-headers"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET", "POST", "PUT"],
                "allowed_origins": ["*"],
                "allowed_headers": ["*"],
                "expose_headers": ["*"],
                "max_age": 7200,
            },
            "expect_fail": False
        },
        id="wildcard-both-origins-and-headers"
    ),

    # ===== HTTP Methods Tests =====
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
                "allowed_origins": ["https://test.com"],
                "allowed_headers": ["*"],
                "expose_headers": [],
                "max_age": 3600,
            },
            "expect_fail": False
        },
        id="all-standard-http-methods"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["POST"],
                "allowed_origins": ["https://site1.com"],
                "allowed_headers": ["Authorization", "Content-Type"],
                "expose_headers": ["x-amz-request-id", "x-custom-header", "x-another-header"],
                "max_age": 1800,
            },
            "expect_fail": False
        },
        id="post-with-multiple-headers"
    ),

    # ===== Validation / Error Tests =====
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET", "FAKE"],
                "allowed_origins": ["*"],
                "allowed_headers": ["*"],
                "expose_headers": ["x-test"],
                "max_age": 3600,
            },
            "expect_fail": True
        },
        id="invalid-method-fake"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["FAKE1", "FAKE2"],
                "allowed_origins": ["*"],
                "allowed_headers": ["*"],
                "expose_headers": [],
                "max_age": 600,
            },
            "expect_fail": True
        },
        id="only-invalid-methods"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 0,
            },
            "expect_fail": False
        },
        id="max-age-zero"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 1,
            },
            "expect_fail": False
        },
        id="max-age-one"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 86400,
            },
            "expect_fail": False
        },
        id="max-age-one-day"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 604800,
            },
            "expect_fail": False
        },
        id="max-age-one-week"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": -1,
            },
            "expect_fail": True
        },
        id="negative-max-age"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 2147483647,
            },
            "expect_fail": False
        },
        id="very-large-max-age"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": [],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 1234,
            },
            "expect_fail": True
        },
        id="empty-allowed-methods"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["get", "POST", "Put"],
                "allowed_origins": ["https://test.com"],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 1234,
            },
            "expect_fail": True
        },
        id="case-sensitive-methods"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["X-Custom-Header", "x-test_header", "X-Header-With-123"],
                "expose_headers": ["X-Expose-Header", "x-another_header"],
                "max_age": 1234,
            },
            "expect_fail": False
        },
        id="special-characters-in-headers"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["*"],
                "allowed_headers": ["X-Very-Long-Header-Name-" + "A" * 100],
                "expose_headers": ["X-Very-Long-Header-Name-" + "A" * 100],
                "max_age": 1234,
            },
            "expect_fail": False
        },
        id="very-long-header-names"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                "allowed_origins": ["https://app.example.com"],
                "allowed_headers": [
                    "Authorization", "Content-Type", "X-Requested-With",
                    "Accept", "Origin", "Access-Control-Request-Method",
                    "Access-Control-Request-Headers"
                ],
                "expose_headers": [
                    "ETag", "x-amz-request-id", "x-amz-id-2",
                    "x-amz-version-id", "x-amz-delete-marker"
                ],
                "max_age": 3600,
            },
            "expect_fail": False
        },
        id="common-headers-combination"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": [
                    "https://secure.com",
                    "http://insecure.com",
                ],
                "allowed_headers": ["Authorization"],
                "expose_headers": ["x-custom-header"],
                "max_age": 1234,
            },
            "expect_fail": False
        },
        id="different-protocols"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["OPTIONS", "GET", "POST"],
                "allowed_origins": ["https://webapp.com"],
                "allowed_headers": ["*"],
                "expose_headers": [],
                "max_age": 86400,
            },
            "expect_fail": False
        },
        id="options-method-preflight"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://valid.com", ""],
                "allowed_headers": ["Authorization", ""],
                "expose_headers": ["", "x-custom"],
                "max_age": 1234,
            },
            "expect_fail": True
        },
        id="edge-case-empty-strings"
    ),
    pytest.param(
        {
            "args": {
                "allowed_methods": ["GET"],
                "allowed_origins": ["https://allowed.com"],
                "allowed_headers": ["X-Header-ção", "X-测试-Header"],
                "expose_headers": ["X-Exposé-Header"],
                "max_age": 1234,
            },
            "expect_fail": False
        },
        id="unicode-in-headers"
    ),
]

@pytest.mark.aws
@pytest.mark.parametrize("test_case", cors_test_cases)
def test_cors_put_get_with_awscli(existing_bucket_name, profile_name, test_case):
    """Test CORS configuration using AWS CLI."""
    bucket_name = existing_bucket_name
    cors_args = test_case["args"]
    expect_fail = test_case["expect_fail"]

    cors_config = change_cors_json(bucket_name=bucket_name, cors_args=cors_args)
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp_file:
        json.dump(cors_config, tmp_file)
        tmp_file.flush()
        tmp_file_path = tmp_file.name

    put_cmd = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file_path} --profile {profile_name}"
    get_cmd = f"aws s3api get-bucket-cors --bucket {bucket_name} --profile {profile_name}"

    put_result = execute_subprocess(put_cmd, expected_failure=expect_fail)

    if expect_fail:
        assert put_result.returncode != 0, f"Expected fail, but returned 0: {put_result.stdout}"
        return
    else:
        assert put_result.returncode == 0, f"Command failed: {put_result.stderr}"

    get_result = execute_subprocess(get_cmd)
    assert get_result.returncode == 0, f"Failed to fetch CORS: {get_result.stderr}"

    cors = json.loads(get_result.stdout)["CORSRules"][0]

    # Validate allowed methods
    assert sorted(cors.get("AllowedMethods", [])) == sorted(cors_args.get("allowed_methods", []))
    
    # Validate allowed origins
    assert sorted(cors.get("AllowedOrigins", [])) == sorted(cors_args.get("allowed_origins", []))

    # Validate allowed headers
    if cors_args.get("allowed_headers"):
        assert sorted(cors.get("AllowedHeaders", [])) == sorted(cors_args.get("allowed_headers", []))
    else:
        assert "AllowedHeaders" not in cors or not cors.get("AllowedHeaders")

    # Validate expose headers
    if cors_args.get("expose_headers"):
        assert sorted(cors.get("ExposeHeaders", [])) == sorted(cors_args.get("expose_headers", []))
    else:
        assert "ExposeHeaders" not in cors or not cors.get("ExposeHeaders")

    # Validate max age
    if "max_age" in cors_args:
        assert cors.get("MaxAgeSeconds") == cors_args["max_age"]


@pytest.mark.aws
def test_cors_configuration_replacement_cli(existing_bucket_name, profile_name):
    """Test that a new CORS configuration completely replaces the previous one using CLI."""
    bucket_name = existing_bucket_name
    
    # First configuration
    first_cors_args = {
        "allowed_methods": ["GET"],
        "allowed_origins": ["https://allowed.com"],
        "allowed_headers": ["Authorization"],
        "expose_headers": ["x-custom-header"],
        "max_age": 1234,
    }
    
    cors_config1 = change_cors_json(bucket_name=bucket_name, cors_args=first_cors_args)
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp_file1:
        json.dump(cors_config1, tmp_file1)
        tmp_file1.flush()
        tmp_file1_path = tmp_file1.name

    put_cmd1 = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file1_path} --profile {profile_name}"
    put_result1 = execute_subprocess(put_cmd1)
    assert put_result1.returncode == 0, f"First command failed: {put_result1.stderr}"
    
    # Second different configuration
    second_cors_args = {
        "allowed_methods": ["POST", "DELETE"],
        "allowed_origins": ["https://different.com"],
        "allowed_headers": ["Content-Type"],
        "expose_headers": [],
        "max_age": 7200,
    }
    
    cors_config2 = change_cors_json(bucket_name=bucket_name, cors_args=second_cors_args)
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp_file2:
        json.dump(cors_config2, tmp_file2)
        tmp_file2.flush()
        tmp_file2_path = tmp_file2.name

    put_cmd2 = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file2_path} --profile {profile_name}"
    put_result2 = execute_subprocess(put_cmd2)
    assert put_result2.returncode == 0, f"Second command failed: {put_result2.stderr}"

    # Verify the old configuration was completely replaced
    get_cmd = f"aws s3api get-bucket-cors --bucket {bucket_name} --profile {profile_name}"
    get_result = execute_subprocess(get_cmd)
    assert get_result.returncode == 0, f"Failed to fetch final CORS: {get_result.stderr}"

    cors = json.loads(get_result.stdout)["CORSRules"][0]
    
    # Verify the new configuration
    assert cors.get("AllowedMethods") == ["POST", "DELETE"]
    assert "GET" not in cors.get("AllowedMethods", [])
    assert cors.get("AllowedOrigins") == ["https://different.com"]
    assert "https://allowed.com" not in cors.get("AllowedOrigins", [])
    assert cors.get("MaxAgeSeconds") == 7200
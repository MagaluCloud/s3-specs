import json
import logging
import tempfile

import pytest

from s3_specs.docs.tools.utils import execute_subprocess

pytestmark = [pytest.mark.homologacao, pytest.mark.cli, pytest.mark.cors]

cors_test_cases = [
    # ===== Basic Tests =====
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": False,
        },
        id="simple-valid-config",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET", "PUT"],
                "AllowedOrigins": ["https://allowed.com", "https://other.com"],
                "AllowedHeaders": ["Authorization", "Content-Type"],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 3600,
            },
            "expect_fail": False,
        },
        id="multiple-methods-and-origins",
    ),
    # ===== Wildcard Tests =====
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": [],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 300,
            },
            "expect_fail": True,
        },
        id="wildcard-header-no-origins",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": [],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 0,
            },
            "expect_fail": False,
        },
        id="wildcard-origin-empty-headers",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET", "POST", "PUT"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["*"],
                "MaxAgeSeconds": 7200,
            },
            "expect_fail": False,
        },
        id="wildcard-both-origins-and-headers",
    ),
    # ===== HTTP Methods Tests =====
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
                "AllowedOrigins": ["https://test.com"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 3600,
            },
            "expect_fail": False,
        },
        id="all-standard-http-methods",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["POST"],
                "AllowedOrigins": ["https://site1.com"],
                "AllowedHeaders": ["Authorization", "Content-Type"],
                "ExposeHeaders": [
                    "x-amz-request-id",
                    "x-custom-header",
                    "x-another-header",
                ],
                "MaxAgeSeconds": 1800,
            },
            "expect_fail": False,
        },
        id="post-with-multiple-headers",
    ),
    # ===== Validation / Error Tests =====
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET", "FAKE"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["x-test"],
                "MaxAgeSeconds": 3600,
            },
            "expect_fail": True,
        },
        id="invalid-method-fake",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["FAKE1", "FAKE2"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 600,
            },
            "expect_fail": True,
        },
        id="only-invalid-methods",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 0,
            },
            "expect_fail": False,
        },
        id="max-age-zero",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 1,
            },
            "expect_fail": False,
        },
        id="max-age-one",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 86400,
            },
            "expect_fail": False,
        },
        id="max-age-one-day",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 604800,
            },
            "expect_fail": False,
        },
        id="max-age-one-week",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": -1,
            },
            "expect_fail": True,
        },
        id="negative-max-age",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 2147483647,
            },
            "expect_fail": False,
        },
        id="very-large-max-age",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": [],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": True,
        },
        id="empty-allowed-methods",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["get", "POST", "Put"],
                "AllowedOrigins": ["https://test.com"],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": True,
        },
        id="case-sensitive-methods",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": [
                    "X-Custom-Header",
                    "x-test_header",
                    "X-Header-With-123",
                ],
                "ExposeHeaders": ["X-Expose-Header", "x-another_header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": False,
        },
        id="special-characters-in-headers",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["X-Very-Long-Header-Name-" + "A" * 100],
                "ExposeHeaders": ["X-Very-Long-Header-Name-" + "A" * 100],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": False,
        },
        id="very-long-header-names",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                "AllowedOrigins": ["https://app.example.com"],
                "AllowedHeaders": [
                    "Authorization",
                    "Content-Type",
                    "X-Requested-With",
                    "Accept",
                    "Origin",
                    "Access-Control-Request-Method",
                    "Access-Control-Request-Headers",
                ],
                "ExposeHeaders": [
                    "ETag",
                    "x-amz-request-id",
                    "x-amz-id-2",
                    "x-amz-version-id",
                    "x-amz-delete-marker",
                ],
                "MaxAgeSeconds": 3600,
            },
            "expect_fail": False,
        },
        id="common-headers-combination",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": [
                    "https://secure.com",
                    "http://insecure.com",
                ],
                "AllowedHeaders": ["Authorization"],
                "ExposeHeaders": ["x-custom-header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": False,
        },
        id="different-protocols",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["OPTIONS", "GET", "POST"],
                "AllowedOrigins": ["https://webapp.com"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": [],
                "MaxAgeSeconds": 86400,
            },
            "expect_fail": False,
        },
        id="options-method-preflight",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://valid.com", ""],
                "AllowedHeaders": ["Authorization", ""],
                "ExposeHeaders": ["", "x-custom"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": True,
        },
        id="edge-case-empty-strings",
    ),
    pytest.param(
        {
            "args": {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["https://allowed.com"],
                "AllowedHeaders": ["X-Header-ção", "X-测试-Header"],
                "ExposeHeaders": ["X-Exposé-Header"],
                "MaxAgeSeconds": 1234,
            },
            "expect_fail": False,
        },
        id="unicode-in-headers",
    ),
]


@pytest.mark.aws
@pytest.mark.parametrize("test_case", cors_test_cases)
def test_cors_put_get_with_awscli(existing_bucket_name, profile_name, test_case):
    """Test CORS configuration using AWS CLI."""
    bucket_name = existing_bucket_name
    cors_args = test_case["args"]
    expect_fail = test_case["expect_fail"]

    cors_config = {"CORSRules": [cors_args]}
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".json", delete=False
    ) as tmp_file:
        json.dump(cors_config, tmp_file)
        tmp_file.flush()
        tmp_file_path = tmp_file.name

    put_cmd = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file_path} --profile {profile_name}"
    get_cmd = (
        f"aws s3api get-bucket-cors --bucket {bucket_name} --profile {profile_name}"
    )

    put_result = execute_subprocess(put_cmd, expected_failure=expect_fail)

    if expect_fail:
        assert (
            put_result.returncode != 0
        ), f"Expected fail, but returned 0: {put_result.stdout}"
        return
    else:
        assert put_result.returncode == 0, f"Command failed: {put_result.stderr}"

    get_result = execute_subprocess(get_cmd)
    assert get_result.returncode == 0, f"Failed to fetch CORS: {get_result.stderr}"

    cors = json.loads(get_result.stdout)["CORSRules"][0]

    # Validate allowed methods
    assert sorted(cors.get("AllowedMethods", [])) == sorted(
        cors_args.get("AllowedMethods", [])
    )

    # Validate allowed origins
    assert sorted(cors.get("AllowedOrigins", [])) == sorted(
        cors_args.get("AllowedOrigins", [])
    )

    # Validate allowed headers
    if cors_args.get("AllowedHeaders"):
        assert sorted(cors.get("AllowedHeaders", [])) == sorted(
            cors_args.get("AllowedHeaders", [])
        )
    else:
        assert "AllowedHeaders" not in cors or not cors.get("AllowedHeaders")

    # Validate expose headers
    if cors_args.get("ExposeHeaders"):
        assert sorted(cors.get("ExposeHeaders", [])) == sorted(
            cors_args.get("ExposeHeaders", [])
        )
    else:
        assert "ExposeHeaders" not in cors or not cors.get("ExposeHeaders")

    # Validate max age
    if "MaxAgeSeconds" in cors_args:
        assert cors.get("MaxAgeSeconds") == cors_args["MaxAgeSeconds"]


@pytest.mark.aws
def test_cors_configuration_replacement_cli(existing_bucket_name, profile_name):
    """Test that a new CORS configuration completely replaces the previous one using CLI."""
    bucket_name = existing_bucket_name

    # First configuration
    first_cors_args = {
        "AllowedMethods": ["GET"],
        "AllowedOrigins": ["https://allowed.com"],
        "AllowedHeaders": ["Authorization"],
        "ExposeHeaders": ["x-custom-header"],
        "MaxAgeSeconds": 1234,
    }

    # First configuration - create CORS config directly
    cors_config1 = {"CORSRules": [first_cors_args]}
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".json", delete=False
    ) as tmp_file1:
        json.dump(cors_config1, tmp_file1)
        tmp_file1.flush()
        tmp_file1_path = tmp_file1.name

    put_cmd1 = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file1_path} --profile {profile_name}"
    put_result1 = execute_subprocess(put_cmd1)
    assert put_result1.returncode == 0, f"First command failed: {put_result1.stderr}"

    # Second different configuration
    second_cors_args = {
        "AllowedMethods": ["POST", "DELETE"],
        "AllowedOrigins": ["https://different.com"],
        "AllowedHeaders": ["Content-Type"],
        "ExposeHeaders": [],
        "MaxAgeSeconds": 7200,
    }

    # Second configuration - create CORS config directly
    cors_config2 = {"CORSRules": [second_cors_args]}
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".json", delete=False
    ) as tmp_file2:
        json.dump(cors_config2, tmp_file2)
        tmp_file2.flush()
        tmp_file2_path = tmp_file2.name

    put_cmd2 = f"aws s3api put-bucket-cors --bucket {bucket_name} --cors-configuration file://{tmp_file2_path} --profile {profile_name}"
    put_result2 = execute_subprocess(put_cmd2)
    assert put_result2.returncode == 0, f"Second command failed: {put_result2.stderr}"

    # Verify the old configuration was completely replaced
    get_cmd = (
        f"aws s3api get-bucket-cors --bucket {bucket_name} --profile {profile_name}"
    )
    get_result = execute_subprocess(get_cmd)
    assert (
        get_result.returncode == 0
    ), f"Failed to fetch final CORS: {get_result.stderr}"

    cors = json.loads(get_result.stdout)["CORSRules"][0]

    # Verify the new configuration
    assert cors.get("AllowedMethods") == ["POST", "DELETE"]
    assert "GET" not in cors.get("AllowedMethods", [])
    assert cors.get("AllowedOrigins") == ["https://different.com"]
    assert "https://allowed.com" not in cors.get("AllowedOrigins", [])
    assert cors.get("MaxAgeSeconds") == 7200

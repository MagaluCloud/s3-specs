import os, pytest, logging
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import run_example, change_cors_json

# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # CORS Configuration (Cross-Origin Resource Sharing)
#
# By default, requests to an object storage bucket from web applications running in different domains
# are blocked due to browser security policies (Same-Origin Policy).
# 
# **CORS (Cross-Origin Resource Sharing)** is a mechanism that allows a bucket owner to control
# how resources stored in a bucket can be accessed from different origins (domains).
# 
# CORS configuration is defined at the bucket level and consists of one or more `CORSRule` blocks, 
# each describing which HTTP methods, origins, headers, and response headers are allowed.
#
# A typical use case for enabling CORS is when a web frontend (e.g., running on `https://example.com`)
# needs to directly access assets (images, videos, APIs) stored in a storage bucket via JavaScript.
#
# CORS settings help the browser validate whether a cross-origin request is safe and permitted.
#
# Without proper CORS configuration, such requests will be rejected by the browser, even if the
# bucket policy allows them at the server level.
#
# **Important:** CORS does not replace the need for proper access control policies (bucket policies or ACL).
# It complements them by ensuring browser-level validation.

# + tags=["parameters"]
pytestmark = [pytest.mark.homologacao, pytest.mark.cors]
config = os.getenv("CONFIG", "../params/br-ne1.yaml")


def is_client_error_with_code(obj, code: str) -> bool:
    """Check if the given object is a ClientError with the specified error code."""
    if not isinstance(obj, ClientError):
        return False
    actual_code = obj.response.get("Error", {}).get("Code")
    if code == "InvalidArgument":
        return actual_code in ["InvalidArgument", "MalformedXML"]
    return actual_code == code


@pytest.fixture
def base_cors_args():
    """Return a base dictionary of valid CORS configuration arguments."""
    return {
        "allowed_methods": ["GET"],
        "allowed_origins": ["https://allowed.com"],
        "allowed_headers": ["Authorization"],
        "expose_headers": ["x-custom-header"],
        "max_age": 1234,
    }


def apply_and_get_cors(s3_client, bucket_name, cors_args):
    """Apply CORS configuration to the bucket and retrieve the applied rules. """
    cors_config = change_cors_json(bucket_name=bucket_name, cors_args=cors_args)
    try:
        resp = s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
        assert resp['ResponseMetadata']['HTTPStatusCode'] in (200, 204)
    except ClientError as e:
        return e
    return s3_client.get_bucket_cors(Bucket=bucket_name).get("CORSRules", [{}])[0]


# ===== Basic Tests =====

def test_simple_valid_config(s3_client, existing_bucket_name, base_cors_args):
    """Tests a basic valid CORS configuration."""
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert sorted(result.get("AllowedMethods", [])) == ["GET"]
    assert result.get("AllowedOrigins") == ["https://allowed.com"]
    assert result.get("AllowedHeaders") == ["Authorization"]
    assert result.get("ExposeHeaders") == ["x-custom-header"]
    assert result.get("MaxAgeSeconds") == 1234


def test_multiple_methods_and_origins(s3_client, existing_bucket_name, base_cors_args):
    """Tests multiple HTTP methods and origins."""
    base_cors_args.update({
        "allowed_methods": ["GET", "PUT"],
        "allowed_origins": ["https://allowed.com", "https://other.com"],
        "allowed_headers": ["Authorization", "Content-Type"],
        "expose_headers": [],
        "max_age": 3600,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert sorted(result.get("AllowedMethods")) == ["GET", "PUT"]
    assert sorted(result.get("AllowedOrigins")) == ["https://allowed.com", "https://other.com"]
    assert sorted(result.get("AllowedHeaders")) == ["Authorization", "Content-Type"]
    assert result.get("ExposeHeaders") is None
    assert result.get("MaxAgeSeconds") == 3600


# ===== Wildcard Tests =====

def test_wildcard_header_no_origins(s3_client, existing_bucket_name, base_cors_args):
    """Tests wildcard in headers without specific origins (should error)."""
    base_cors_args.update({
        "allowed_origins": [],
        "allowed_headers": ["*"],
        "expose_headers": [],
        "max_age": 300,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_wildcard_origin_empty_headers(s3_client, existing_bucket_name, base_cors_args):
    """Tests wildcard in origins with empty headers."""
    base_cors_args.update({
        "allowed_origins": ["*"],
        "allowed_headers": [],
        "expose_headers": [],
        "max_age": 0,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert result.get("AllowedOrigins") == ["*"]
    assert result.get("AllowedHeaders") is None
    assert result.get("ExposeHeaders") is None
    assert result.get("MaxAgeSeconds") == 0


def test_wildcard_both_origins_and_headers(s3_client, existing_bucket_name, base_cors_args):
    """Tests wildcard in both origins and headers."""
    base_cors_args.update({
        "allowed_methods": ["GET", "POST", "PUT"],
        "allowed_origins": ["*"],
        "allowed_headers": ["*"],
        "expose_headers": ["*"],
        "max_age": 7200,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert sorted(result.get("AllowedMethods")) == ["GET", "POST", "PUT"]
    assert result.get("AllowedOrigins") == ["*"]
    assert result.get("AllowedHeaders") == ["*"]
    assert result.get("ExposeHeaders") == ["*"]
    assert result.get("MaxAgeSeconds") == 7200


# ===== HTTP Methods Tests =====

def test_all_standard_http_methods(s3_client, existing_bucket_name, base_cors_args):
    """Tests all standard HTTP methods."""
    base_cors_args.update({
        "allowed_methods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
        "allowed_origins": ["https://test.com"],
        "allowed_headers": ["*"],
        "expose_headers": [],
        "max_age": 3600,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert sorted(result.get("AllowedMethods")) == ["DELETE", "GET", "HEAD", "POST", "PUT"]


def test_post_with_multiple_headers(s3_client, existing_bucket_name, base_cors_args):
    """Tests POST method with multiple headers."""
    base_cors_args.update({
        "allowed_methods": ["POST"],
        "allowed_origins": ["https://site1.com"],
        "allowed_headers": ["Authorization", "Content-Type"],
        "expose_headers": ["x-amz-request-id", "x-custom-header", "x-another-header"],
        "max_age": 1800,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert result.get("AllowedMethods") == ["POST"]
    assert result.get("AllowedOrigins") == ["https://site1.com"]
    assert sorted(result.get("AllowedHeaders")) == ["Authorization", "Content-Type"]
    assert sorted(result.get("ExposeHeaders")) == [
        "x-amz-request-id", "x-another-header", "x-custom-header"
    ]
    assert result.get("MaxAgeSeconds") == 1800


# ===== Validation / Error Tests =====

def test_invalid_method_fake(s3_client, existing_bucket_name, base_cors_args):
    base_cors_args.update({
        "allowed_methods": ["GET", "FAKE"],
        "allowed_origins": ["*"],
        "allowed_headers": ["*"],
        "expose_headers": ["x-test"],
        "max_age": 3600,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_only_invalid_methods(s3_client, existing_bucket_name, base_cors_args):
    """Tests only invalid HTTP methods."""
    base_cors_args.update({
        "allowed_methods": ["FAKE1", "FAKE2"],
        "allowed_origins": ["*"],
        "allowed_headers": ["*"],
        "expose_headers": [],
        "max_age": 600,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_max_age_boundaries(s3_client, existing_bucket_name, base_cors_args):
    """Tests boundary values for max_age."""
    test_cases = [0, 1, 86400, 604800]  # 0, 1 sec, 1 day, 1 week
    
    for max_age_value in test_cases:
        base_cors_args.update({"max_age": max_age_value})
        result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
        assert not isinstance(result, ClientError)
        assert result.get("MaxAgeSeconds") == max_age_value


def test_negative_max_age(s3_client, existing_bucket_name, base_cors_args):
    """Tests negative max_age."""
    base_cors_args.update({"max_age": -1})
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_very_large_max_age(s3_client, existing_bucket_name, base_cors_args):
    """Tests very large max_age value."""
    base_cors_args.update({"max_age": 2147483647})  # Max int32
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert result.get("MaxAgeSeconds") == 2147483647


def test_empty_allowed_methods(s3_client, existing_bucket_name, base_cors_args):
    """Tests empty list of allowed methods."""
    base_cors_args.update({"allowed_methods": []})
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "MalformedXML")


def test_case_sensitive_methods(s3_client, existing_bucket_name, base_cors_args):
    """Tests case sensitivity in HTTP methods."""
    base_cors_args.update({
        "allowed_methods": ["get", "POST", "Put"],
        "allowed_origins": ["https://test.com"],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_special_characters_in_headers(s3_client, existing_bucket_name, base_cors_args):
    """Tests headers containing special characters."""
    base_cors_args.update({
        "allowed_headers": ["X-Custom-Header", "x-test_header", "X-Header-With-123"],
        "expose_headers": ["X-Expose-Header", "x-another_header"],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert "X-Custom-Header" in result.get("AllowedHeaders")
    assert "x-another_header" in result.get("ExposeHeaders")


def test_very_long_header_names(s3_client, existing_bucket_name, base_cors_args):
    """Tests very long header names."""
    long_header = "X-Very-Long-Header-Name-" + "A" * 100
    base_cors_args.update({
        "allowed_headers": [long_header],
        "expose_headers": [long_header],
        "allowed_origins": ["*"],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert long_header in result.get("AllowedHeaders")
    assert long_header in result.get("ExposeHeaders")


def test_common_headers_combination(s3_client, existing_bucket_name, base_cors_args):
    """Tests common headers combination."""
    base_cors_args.update({
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
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert len(result.get("AllowedHeaders", [])) == 7
    assert len(result.get("ExposeHeaders", [])) == 5


def test_different_protocols(s3_client, existing_bucket_name, base_cors_args):
    """Tests different protocols in allowed origins."""
    base_cors_args.update({
        "allowed_origins": [
            "https://secure.com",
            "http://insecure.com",
        ],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert "https://secure.com" in result.get("AllowedOrigins")
    assert "http://insecure.com" in result.get("AllowedOrigins")


def test_options_method_preflight(s3_client, existing_bucket_name, base_cors_args):
    """Tests configuration specific for preflight OPTIONS requests."""
    base_cors_args.update({
        "allowed_methods": ["OPTIONS", "GET", "POST"],
        "allowed_origins": ["https://webapp.com"],
        "allowed_headers": ["*"],
        "expose_headers": [],
        "max_age": 86400,
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert "OPTIONS" in result.get("AllowedMethods", [])
    assert result.get("MaxAgeSeconds") == 86400


def test_edge_case_empty_strings(s3_client, existing_bucket_name, base_cors_args):
    """Tests empty strings in configuration."""
    base_cors_args.update({
        "allowed_origins": ["https://valid.com", ""],
        "allowed_headers": ["Authorization", ""],
        "expose_headers": ["", "x-custom"],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_unicode_in_headers(s3_client, existing_bucket_name, base_cors_args):
    """Tests unicode characters in header names."""
    base_cors_args.update({
        "allowed_headers": ["X-Header-ção", "X-测试-Header"],
        "expose_headers": ["X-Exposé-Header"],
    })
    result = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result, ClientError)
    assert "X-Header-ção" in result.get("AllowedHeaders")
    assert "X-Exposé-Header" in result.get("ExposeHeaders")


# ===== Clean-up Tests =====

def test_cors_configuration_replacement(s3_client, existing_bucket_name, base_cors_args):
    """Tests that a new CORS configuration completely replaces the previous one."""
    # First configuration
    result1 = apply_and_get_cors(s3_client, existing_bucket_name, base_cors_args)
    assert not isinstance(result1, ClientError)
    
    # Second different configuration
    new_args = {
        "allowed_methods": ["POST", "DELETE"],
        "allowed_origins": ["https://different.com"],
        "allowed_headers": ["Content-Type"],
        "expose_headers": [],
        "max_age": 7200,
    }
    result2 = apply_and_get_cors(s3_client, existing_bucket_name, new_args)
    assert not isinstance(result2, ClientError)
    
    # Verify the old configuration was completely replaced
    assert result2.get("AllowedMethods") == ["POST", "DELETE"]
    assert "GET" not in result2.get("AllowedMethods", [])
    assert result2.get("AllowedOrigins") == ["https://different.com"]
    assert "https://allowed.com" not in result2.get("AllowedOrigins", [])


run_example(__name__, None, config=config)

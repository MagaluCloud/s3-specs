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

test_cases = [
    (
        # Simple valid config with GET method and a single origin
        {"allowed_methods": ["GET"], "allowed_origins": ["https://allowed.com"],
         "allowed_headers": ["Authorization"], "expose_headers": ["x-custom-header"],
         "max_age": 1234},
        ["GET"], ["https://allowed.com"], ["Authorization"], ["x-custom-header"], 1234
    ),
    (
        # Multiple methods and origins with a valid configuration
        {"allowed_methods": ["GET", "PUT"], "allowed_origins": ["https://allowed.com", "https://other.com"],
         "allowed_headers": ["Authorization", "Content-Type"], "expose_headers": [],
         "max_age": 3600},
        ["GET", "PUT"], ["https://allowed.com", "https://other.com"],
        ["Authorization", "Content-Type"], [], 3600
    ),
    (
        # Configuration with wildcard headers and no allowed origins
        {"allowed_methods": ["GET"], "allowed_origins": [], "allowed_headers": ["*"],
         "expose_headers": [], "max_age": 300},
        ["GET"], [], ["*"], [], 300
    ),
    (
        # Allowed origin as "*" (wildcard), empty headers and no exposed headers
        {"allowed_methods": ["GET"], "allowed_origins": ["*"], "allowed_headers": [],
         "expose_headers": [], "max_age": 0},
        ["GET"], ["*"], [], [], 0
    ),
    (
        # Configuration with a fake/unsupported method (to trigger error handling)
        {"allowed_methods": ["GET", "FAKE"], "allowed_origins": ["*"], "allowed_headers": ["*"],
         "expose_headers": ["x-test"], "max_age": 3600},
        ["GET", "FAKE"], ["*"], ["*"], ["x-test"], 3600
    ),
    (
        # Multiple headers and exposed headers with POST method
        {"allowed_methods": ["POST"], "allowed_origins": ["https://site1.com"],
         "allowed_headers": ["Authorization", "Content-Type"],
         "expose_headers": ["x-amz-request-id", "x-custom-header", "x-another-header"],
         "max_age": 1800},
        ["POST"], ["https://site1.com"], ["Authorization", "Content-Type"],
        ["x-amz-request-id", "x-custom-header", "x-another-header"], 1800
    ),
    (
        # Invalid methods only to test error handling path
        {"allowed_methods": ["FAKE1", "FAKE2"], "allowed_origins": ["*"],
         "allowed_headers": ["*"], "expose_headers": [], "max_age": 600},
        ["FAKE1", "FAKE2"], ["*"], ["*"], [], 600
    ),
    (
        # Multiple origins, one of which may be invalid (test normalization and validation)
        {"allowed_methods": ["GET"], "allowed_origins": ["https://valid.com", "https://invalid.com"],
         "allowed_headers": ["Authorization"], "expose_headers": [], "max_age": 1200},
        ["GET"], ["https://valid.com", "https://invalid.com"], ["Authorization"], [], 1200
    ),
]

@pytest.mark.parametrize("test_case", test_cases)
def test_various_cors_configurations(s3_client, existing_bucket_name, test_case):
    """
    Validates different CORS configurations applied to an S3-compatible bucket.

    The test:
    - Applies a CORS config using put_bucket_cors
    - If the config is invalid, it checks that the proper error is raised
    - If accepted, it fetches the config and verifies its persistence and correctness
    """
    cors_args, exp_methods, exp_origins, exp_headers, exp_expose, exp_max_age = test_case
    cors_config = change_cors_json(bucket_name=existing_bucket_name, cors_args=cors_args)
    try:
        resp = s3_client.put_bucket_cors(Bucket=existing_bucket_name, CORSConfiguration=cors_config)
        assert resp['ResponseMetadata']['HTTPStatusCode'] in (200,204)
    except ClientError as e:
        assert e.response['Error']['Code'] in ("MalformedXML","InvalidRequest","InvalidArgument")
        return

    rules = s3_client.get_bucket_cors(Bucket=existing_bucket_name).get("CORSRules",[{}])[0]

    assert sorted(rules.get("AllowedMethods",[])) == sorted(exp_methods)
    allowed_origins = rules.get("AllowedOrigins",[])
    assert (allowed_origins==[] or allowed_origins==["*"]) if not exp_origins else sorted(allowed_origins) == sorted(exp_origins)
    assert sorted(rules.get("AllowedHeaders",[])) == sorted(exp_headers)
    assert sorted(rules.get("ExposeHeaders",[])) == sorted(exp_expose)
    assert rules.get("MaxAgeSeconds",-1) == exp_max_age

run_example(__name__, "test_various_cors_configurations", config=config)

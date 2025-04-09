# + {"jupyter": {"source_hidden": true}}
import logging
import pytest
from s3_specs.docs.s3_helpers import run_example
from shlex import split
import subprocess
from s3_specs.docs.tools.crud import fixture_versioned_bucket, upload_object
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess
import os
from itertools import product 

config = "../params/br-se1.yaml"

# + {"jupyter": {"source_hidden": true}}
pytestmark = [pytest.mark.bucket_versioning, pytest.mark.cli]


# Acl related tests

# Global Variables used by the acl rel tests
acl_list = [
    pytest.param('private', id='acl-private', marks=pytest.mark.acl),
    pytest.param('public-read', id='acl-public-read', marks=pytest.mark.acl),
    pytest.param('public-read-write', id='acl-public-read-write', marks=pytest.mark.acl),
    pytest.param('authenticated-read', id='acl-authenticated-read', marks=pytest.mark.acl)
]

# Define test commands with appropriate markers
commands = [
    pytest.param(
        "mgc object-storage objects upload {file_path} {bucket_name}/{object_key} --no-confirm --raw",
        marks=pytest.mark.mgc,
        id="mgc-upload"
    ),
    pytest.param(
        "aws --profile {profile_name} s3api put-object --bucket {bucket_name} --key {object_key} --body {file_path}",
        marks=pytest.mark.aws,
        id="aws-upload"
    ),
    pytest.param(
        "rclone copy {file_path} {profile_name}:{bucket_name}/{object_key}",
        marks=pytest.mark.rclone,
        id="rclone-upload"
    )
]

@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template",
    [
        pytest.param(acl.values, ''.join(cmd.values), id=f"{cmd.id}-{acl.id}")
        for acl, cmd in product(acl_list, commands)
    ],
    indirect=['fixture_versioned_bucket']
)
def test_set_version_on_bucket_with_acl(s3_client, fixture_bucket_with_name, cmd_template, expected, profile_name):
    """Test versioning enablement through different CLI tools with various ACL settings."""
    bucket_name = fixture_bucket_with_name

    # Enabling versioning through CLI
    cmd = split(cmd_template.format(
        bucket_name=bucket_name, 
        profile_name=profile_name
    ))

    # Executing command
    result = execute_subprocess(cmd)

    # Verify command success
    assert result.returncode == 0, (
        f"Command failed with exit code {result.returncode}\n"
        f"Command: {cmd}\n"
        f"Error: {result.stderr}"
    )
    
    # Retrieving versioning value
    versioning_status = s3_client.get_bucket_versioning(Bucket=bucket_name)
    assert expected == versioning_status.get('Status'), (
        f"Expected versioning status {expected}, got {versioning_status.get('Status')}"
    )


# Define test commands with appropriate markers
# Define test commands with appropriate markers
commands = [
    pytest.param(
        "mgc object-storage objects upload {file_path} {bucket_name}/{object_key} --no-confirm --raw",
        marks=pytest.mark.mgc,
        id="mgc-upload"
    ),
    pytest.param(
        "aws --profile {profile_name} s3api put-object --bucket {bucket_name} --key {object_key} --body {file_path}",
        marks=pytest.mark.aws,
        id="aws-upload"
    ),
    pytest.param(
        "rclone copy {file_path} {profile_name}:{bucket_name}/{object_key}",
        marks=pytest.mark.rclone,
        id="rclone-upload"
    )
]

@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template",
    [
        pytest.param(acl.values, ''.join(cmd.values), id=f"{cmd.id}-{acl.id}")
        for acl, cmd in product(acl_list, commands)
    ],
    indirect=['fixture_versioned_bucket']
)
def test_upload_version_on_bucket_with_acl( s3_client,
                                            fixture_versioned_bucket, 
                                            fixture_create_small_file, 
                                            cmd_template,
                                            profile_name ):
    """
    Test that object version uploads work correctly with different ACL settings.
    
    Steps:
    1. Upload a test file to a versioned S3 bucket using a CLI command.
    2. Verify that:
       - The object is uploaded successfully.
       - Versioning is enabled on the bucket.
       - The ACL permissions are applied correctly.
    
    Args:
        s3_client: Boto3 S3 client (fixture).
        fixture_versioned_bucket: S3 bucket with versioning enabled (fixture).
        fixture_create_small_file: Temporary test file (fixture).
        cmd_template: CLI command template for uploads (fixture).
        profile_name: AWS CLI profile name (fixture).
    """
    bucket_name = fixture_versioned_bucket
    object_name = bucket_name[:20]

    try:
        # Formatting upload command
        formatted_cmd = split(cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_key=object_name,
            file_path=str(fixture_create_small_file)
        ))

        result = execute_subprocess(formatted_cmd)

        # Verify command success
        assert result.returncode == 0, (
            f"Command failed with exit code {result.returncode}\n"
            f"Command: {formatted_cmd}\n"
            f"Error: {result.stderr}"
        )
        
        # Verify version was created
        versions = s3_client.list_object_versions(
            Bucket=bucket_name,
            Prefix=object_name
        ).get('Versions', [])
        
        # Checking the existence of versions
        assert len(versions) >= 1, "No versions created"
        
    except Exception as e:
        pytest.fail(f"Test failed: {str(e)}")


commands = [
    pytest.param(
        'mgc object-storage objects download --dst="{dst_path}" --src="{bucket_name}/{object_key}" --no-confirm --raw',
        marks=pytest.mark.mgc,
        id="mgc-download"
    ),
    pytest.param(
        "aws --profile {profile_name} s3 cp s3://{bucket_name}/{object_key} {dst_path}",
        marks=pytest.mark.aws,
        id="aws-download"
    ),
    pytest.param(
        "rclone copy {profile_name}:{bucket_name}/{object_key} {dst_dir_path}",
        marks=pytest.mark.rclone,
        id="rclone-download"
    )
]




@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template",
    [
        pytest.param(acl.values, ''.join(cmd.values), id=f"{cmd.id}-{acl.id}")
        for acl, cmd in product(acl_list, commands)
    ],
    indirect=['fixture_versioned_bucket']
)
def test_download_version_on_bucket_with_acl(
    s3_client,
    fixture_versioned_bucket,
    fixture_create_small_file,
    cmd_template,
    profile_name,
):
    """
    Test object download functionality with different ACL settings across multiple CLI tools.
    
    Args:
        s3_client: Boto3 S3 client fixture
        fixture_versioned_bucket: Versioned S3 bucket fixture
        fixture_create_small_file: Temporary test file fixture
        cmd_template: CLI command template
        profile_name: AWS profile name fixture
        tmp_path: Pytest temporary directory fixture
    """
    # Setup test variables
    bucket_name = fixture_versioned_bucket
    object_key = "download_object" # Consistent object key
    source_path = str(fixture_create_small_file)
    download_path = str(fixture_create_small_file)
    dir_path = os.path.dirname(download_path)

    # Test Setup
    upload_response = upload_object(
        s3_client,
        bucket_name=bucket_name,
        object_key=object_key,
        body_file=source_path
    )
    
    # Format and execute download command
    formatted_cmd = cmd_template.format(
        dst_path = download_path,
        dst_dir_path = dir_path, # Rclone Temporary directory 
        bucket_name = bucket_name,
        object_key = object_key,
        profile_name = profile_name        
    )
    
    # Executing subprocess and capturing possible errors
    result = execute_subprocess(formatted_cmd)
    
    # Verify downloaded content
    try:
        with open(source_path, 'rb') as src, open(download_path, 'rb') as dst:
            assert src.read() == dst.read(), "Downloaded content differs from original"
    except FileNotFoundError:
        pytest.fail(f"Downloaded file not found at {download_path}")
    except Exception as e:
        pytest.fail(f"File comparison failed: {str(e)}")

# Tests responsible to check the behavior of deleting versions throught mgc, aws and rclone clis

commands = [
    ("mgc object-storage objects delete {bucket_name}/{object_key} --no-confirm --raw", ""),
    ("aws --profile {profile_name} s3 rm s3://{bucket_name}/{object_key}", "delete: s3://{bucket_name}/{object_key}\n"),
    ("rclone delete {profile_name}:{bucket_name}/{object_key}", "")
]

@pytest.mark.parametrize("cmd_template, expected", commands)
def test_delete_object_with_versions(cmd_template, expected, s3_client, versioned_bucket_with_one_object, profile_name, active_mgc_workspace):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    #Adicionando uma segunda versão deste objeto
    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"second version of this object"
    )

    
    cmd = split(cmd_template.format(bucket_name=bucket_name, profile_name=profile_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")
    

    assert result.stdout == expected.format(bucket_name=bucket_name, object_key=object_key)

run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

commands = [
    ("mgc object-storage buckets delete {bucket_name} --no-confirm --recursive --raw", "the bucket may not be empty"),
    ("aws --profile {profile_name} s3 rb s3://{bucket_name}", "BucketNotEmpty"),
    ("rclone rmdir {profile_name}:{bucket_name}", "BucketNotEmpty")
]

@pytest.mark.parametrize("cmd_template, expected", commands)
def test_delete_bucket_with_objects_with_versions(cmd_template, expected, s3_client, versioned_bucket_with_one_object, profile_name, active_mgc_workspace):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"v2"
    )

    cmd = split(cmd_template.format(bucket_name=bucket_name, profile_name=profile_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")
    assert expected in result.stderr
    
run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

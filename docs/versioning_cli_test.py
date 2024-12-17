import logging
import pytest
from s3_helpers import run_example
from botocore.exceptions import ClientError
from shlex import split, quote
import subprocess

config = "../params/br-se1.yaml"


pytestmark = [pytest.mark.bucket_versioning, pytest.mark.bucket_versioning_cli]


commands = [
    "mgc object-storage objects delete {bucket_name}/{object_key} --no-confirm",
    "aws --profile {profile_name} s3 rm s3://{bucket_name}/{object_key}",
    "rclone delete {profile_name}:{bucket_name}/{object_key}"
]

# + {"jupyter": {"source_hidden": true}}
@pytest.mark.parametrize("cmd_template", commands)
def test_delete_object_with_versions(cmd_template, s3_client, versioned_bucket_with_one_object, profile_name, active_mgc_workspace):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"v2"
    )

    
    cmd = split(cmd_template.format(bucket_name=bucket_name, profile_name=profile_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")
    
    if cmd[0] == "aws":
        assert result.stdout == f"delete: s3://{bucket_name}/{object_key}\n"
    else:
        assert result.stdout == ""

run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

commands = [
    "mgc object-storage buckets delete {bucket_name} --no-confirm --recursive --raw",
    "aws --profile {profile_name} s3 rb s3://{bucket_name}",
    "rclone rmdir {profile_name}:{bucket_name}"
]

# + {"jupyter": {"source_hidden": true}}
@pytest.mark.parametrize("cmd_template", commands)
def test_delete_bucket_with_objects_with_versions(cmd_template, s3_client, versioned_bucket_with_one_object, profile_name, active_mgc_workspace):
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
    if cmd[0] != "mgc":
        assert "BucketNotEmpty" in result.stderr

run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

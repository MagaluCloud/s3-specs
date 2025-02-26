# + {"jupyter": {"source_hidden": true}}
import logging
import pytest
from s3_helpers import run_example
from botocore.exceptions import ClientError
from shlex import split, quote
import subprocess

config = "../params/br-se1.yaml"


# + {"jupyter": {"source_hidden": true}}
pytestmark = [pytest.mark.bucket_versioning, pytest.mark.cli, pytest.mark.prod]


commands = [
    ("mgc object-storage objects delete {bucket_name}/{object_key} --no-confirm", ""),
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

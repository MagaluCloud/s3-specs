# + {"jupyter": {"source_hidden": true}}
import pytest
import json
import logging
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess
from s3_specs.docs.tools.crud import fixture_bucket_with_name
import os
import subprocess
from itertools import product

config = "../params/br-se1.yaml"

# + {"jupyter": {"source_hidden": true}}
pytestmark = [pytest.mark.cold_storage, pytest.mark.cli]
config = "../params/br-se1.yaml"

storage_classes_list = [
    {
        "mgc": "standard",
        "aws": "STANDARD",
        "rclone": "standard",
        "expected": "STANDARD"
    },

    {
        "mgc": "cold",
        "aws": "COLD",
        "rclone": "cold",
        "expected": "COLD_INSTANT"
    },

    {
        "mgc": "glacier_ir",
        "aws": "GLACIER_IR",
        "rclone": "cold_instant",
        "expected": "COLD_INSTANT"
    },

    {
        "mgc": "cold_instant",
        "aws": "COLD_INSTANT",
        "rclone": "cold_instant",
        "expected": "COLD_INSTANT"
    },
]
acl_list = [
    pytest.param('private', id='acl-private', marks=pytest.mark.acl),
    pytest.param('public-read', id='acl-public-read', marks=pytest.mark.acl),
    pytest.param('public-read-write', id='acl-public-read-write', marks=pytest.mark.acl),
    pytest.param('authenticated-read', id='acl-authenticated-read', marks=pytest.mark.acl)
]
head_commands = [
    pytest.param(
        {
            "command": "mgc object-storage objects head --dst {bucket_name}/{object_key} --no-confirm --raw --output json",
        },
        marks=pytest.mark.mgc,
        id="head-mgc"
    ),
    pytest.param(
        {
            "command": "aws s3api head-object --bucket {bucket_name} --key {object_key} --profile {profile_name} --output json",
        },
        marks=pytest.mark.aws,
        id="head-aws"
    ),
    pytest.param(
        {
            "command": "rclone lsl {profile_name}:{bucket_name}/{object_key}",
        },
        marks=pytest.mark.rclone,
        id="head-rclone"
    )
]

# commands for uploading different storage classes
commands = [
     pytest.param(
        {
            "command": "mgc object-storage objects upload --src {file_path} --dst {bucket_name}/{object_key}  --storage-class={storage_class_mgc} --no-confirm --raw --output json",
        },
        marks=pytest.mark.mgc,
        id="upload-mgc"
    ),
    pytest.param(
        {
            "command":"aws s3api put-object --bucket {bucket_name} --key {object_key} --body {file_path} --storage-class {storage_class_aws} --profile {profile_name} --output json",
        },
        marks=pytest.mark.aws,
        id="upload-aws"
        ),
    pytest.param(
        {
            "command": "rclone copyto --s3-storage-class={storage_class_rclone} {file_path} {profile_name}:{bucket_name}/{object_key} --no-check-certificate",
            "expected": "Enabled"
        },
        marks=pytest.mark.rclone,
        id="upload-rclone"
    )
]

@pytest.mark.parametrize(
    "fixture_bucket_with_name, cmd_template, head_template",
    [
        pytest.param(
            "private",  # Fixture needs acl
            cmd.values[0]["command"],
            head.values[0]["command"],
            marks=cmd.marks,
            id=f"storage-classes-{cmd.id}-{head.id}"
        ) for cmd, head in zip(commands, head_commands)
    ],  # Close the list here
    indirect=["fixture_bucket_with_name"]
)
def test_upload_storage_class_cli(s3_client, active_mgc_workspace, profile_name, fixture_bucket_with_name, fixture_create_small_file, cmd_template,head_template):
    """
    Test upload to cold storage
    """
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_key = os.path.basename(file_path)

    # Putting all possible storage classes on the same bucket
    for storage_class in storage_classes_list:
        logging.info(f"Testing storage class: {storage_class['mgc']}")
        expected = storage_class["expected"]    # Expected result
        object_key = f"{object_key}-{storage_class['expected']}"   # Avoid colision
        try:
            # Format and execute the command
            formatted_cmd = cmd_template.format(
                bucket_name=bucket_name,
                profile_name=profile_name,
                file_path=file_path,
                object_key=object_key,
                storage_class_mgc=storage_class["mgc"], # Redundancy need since they have different equivalent arguments
                storage_class_aws=storage_class["aws"],
                storage_class_rclone=storage_class["rclone"]
            )
            result = execute_subprocess(formatted_cmd)

            # Check the output while testing the head cli command
            formatted_head_cmd = head_template.format(
                bucket_name=bucket_name,
                profile_name=profile_name,
                object_key=object_key
            )
            head_result = execute_subprocess(formatted_head_cmd)
            # Check the output
            assert expected in json.dumps(head_result.stdout), pytest.fail(f"Expected {expected} in head result, got {head_result.stdout}")
            logging.info(f"Command executed successfully: {formatted_cmd}")

        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed with error: {e}")
            pytest.fail(f"Command execution failed: {e}")

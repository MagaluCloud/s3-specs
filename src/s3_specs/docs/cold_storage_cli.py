# + {"jupyter": {"source_hidden": true}}
import pytest
import json
import logging
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess, get_different_profile_from_default
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
def test_upload_storage_class_cli(active_mgc_workspace, profile_name, fixture_bucket_with_name, fixture_create_small_file, cmd_template,head_template):
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
# Unable to do following tests with mgc since it doesnt implements profile selector 
# Testing acl operations on storage classes: Upload, Head and List all done by a second profile
commands = [
  #   pytest.param(
  #      {
  #          "command": "mgc object-storage objects upload --src {file_path} --dst {bucket_name}/{object_key}  --storage-class={storage_class_mgc} --no-confirm --raw --output json",
  #      },
  #      marks=pytest.mark.mgc,
  #      id="upload-mgc"
  #  ),
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
head_commands = [
    #pytest.param(
    #    {
    #        "command": "mgc object-storage objects head --dst {bucket_name}/{object_key} --no-confirm --raw --output json",
    #    },
    #    marks=pytest.mark.mgc,
    #    id="head-mgc"
    #),
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
list_commands = [
    #pytest.param(
    #    {
    #        "command": "mgc object-storage objects list {bucket_name} --no-confirm --raw --output json",
    #    },
    #    marks=pytest.mark.mgc,
    #    id="list-mgc"
    #),
    pytest.param(
        {
            "command": "aws s3api list-objects --bucket {bucket_name} --profile {profile_name} --output json",
        },
        marks=pytest.mark.aws,
        id="list-aws"
    ),
    pytest.param(
        {
            "command": "rclone lsl {profile_name}:{bucket_name}",
        },
        marks=pytest.mark.rclone,
        id="list-rclone"
    )
]
acl_list = [
    pytest.param(
        {
            "acl": "private",
            "expected_upload": ["AccessDenied", "Forbidden"],
            "expected_head": ["AccessDenied", "Forbidden"],
            "expected_list": ["AccessDenied", "Forbidden"],
        },
        id="acl-private",
        marks=pytest.mark.acl,
    ),
    #pytest.param(
    #    {
    #        "acl": "public-read",
    #        "expected_upload": "public-read",
    #        "expected_head": "public-read",
    #        "expected_list": "public-read",
    #    },
    #    id="acl-public-read",
    #    marks=pytest.mark.acl,
    #),
    #pytest.param(
    #    {
    #        "acl": "public-read-write",
    #        "expected_upload": "public-read-write",
    #        "expected_head": "public-read-write",
    #        "expected_list": "public-read-write",
    #    },
    #    id="acl-public-read-write",
    #    marks=pytest.mark.acl,
    #),
    #pytest.param(
    #    {
    #        "acl": "authenticated-read",
    #        "expected_upload": "authenticated-read",
    #        "expected_head": "authenticated-read",
    #        "expected_list": "authenticated-read",
    #    },
    #    id="acl-authenticated-read",
    #    marks=pytest.mark.acl,
    #),
]

@pytest.mark.parametrize(
    "fixture_bucket_with_name, acl, cmd_template, head_template, list_template",
    [
        pytest.param(
            acl.values[0]["acl"],  # Fixture needs acl
            acl.values[0],
            cmd[0].values[0]["command"],
            cmd[1].values[0]["command"],
            cmd[2].values[0]["command"],
            #marks=acl.marks,
            id=f"{acl.id}--storage-classes-{cmd[0].id}"
        ) for acl, cmd in product(acl_list, zip(commands, head_commands, list_commands))
    ],
    indirect=["fixture_bucket_with_name"]
)
def test_upload_storage_class_acl_cli(get_different_profile_from_default, active_mgc_workspace, fixture_bucket_with_name, fixture_create_small_file, acl,cmd_template, head_template, list_template):
    """
    Test ACL operations on storage classes: Upload, Head, and List using a second profile.
    """
    default_profile, second_profile = get_different_profile_from_default
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_key = os.path.basename(file_path)

    for storage_class in storage_classes_list:
        logging.info(f"Testing storage class: {storage_class['mgc']}")
        object_key_with_class = f"{object_key}-{storage_class['expected']}"

        try:
            # Format and execute the upload command
            formatted_cmd = cmd_template.format(
                bucket_name=bucket_name,
                profile_name=second_profile,
                file_path=file_path,
                object_key=object_key_with_class,
                storage_class_aws=storage_class["aws"],
                storage_class_rclone=storage_class["rclone"]
            )
            result_upload = execute_subprocess(formatted_cmd, True)
            
            # Testing Upload on acl storage classes
            assert any(expected in result_upload.stderr for expected in acl["expected_upload"]), pytest.fail(
                f"Object was uploaded, got {result_upload.stdout}"
            )

            # Setup for the head and list command
            formatted_default_upload_cmd = cmd_template.format(
                bucket_name=bucket_name,
                profile_name=default_profile,
                file_path=file_path,
                object_key=object_key_with_class,
                storage_class_aws=storage_class["aws"],
                storage_class_rclone=storage_class["rclone"]
            )
            result_upload = execute_subprocess(formatted_default_upload_cmd)


            # Verify the upload using the head command
            formatted_head_cmd = head_template.format(
                bucket_name=bucket_name,
                profile_name=second_profile,
                object_key=object_key_with_class
            )
            head_result = execute_subprocess(formatted_head_cmd, True)

            # Testing Head on acl storage classes
            assert any(expected in head_result.stderr for expected in acl["expected_head"]), pytest.fail(
                f"Expected one of {acl['expected_head']} in head result, got {head_result.stderr}"
            )
            
            # Verify the list command
            formatted_list_cmd = list_template.format(
                bucket_name=bucket_name,
                profile_name=second_profile
            )
            list_result = execute_subprocess(formatted_list_cmd, True)
            
            # Testing List on acl storage classes
            assert any(expected in list_result.stderr for expected in acl["expected_list"]), pytest.fail(
                f"Expected one of {acl['expected_list']} in list result, got {list_result.stderr}"
            )
            
            logging.info(f"List command executed successfully: {formatted_list_cmd}")

        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed with error: {e}")
            pytest.fail(f"Command execution failed: {e}")
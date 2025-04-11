# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
from itertools import product
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.utils import fixture_create_big_file, fixture_create_small_file, execute_subprocess
from s3_specs.docs.tools.crud import fixture_bucket_with_name ,list_all_objects
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

config = "../params/br-se1.yaml"

pytestmark = [pytest.mark.big_objects, pytest.mark.cli]


size_list = [
    {'size': 10, 'unit': 'mb'},
    {'size': 100, 'unit': 'mb'},
    {'size': 1, 'unit': 'gb'},
    {'size': 5, 'unit': 'gb'},
    {'size': 10, 'unit': 'gb'},
]

# Upload big objects to S3 using different CLI tools
commands = [
    pytest.param(
        {"command": "mgc object-storage objects upload {src_path} --dst {bucket_name}/{object_key} --no-confirm --raw",
         "expected": "Upload successful"},
        marks=pytest.mark.mgc,
        id="mgc-upload-big"
    ),
    pytest.param(
        {"command": "aws --profile {profile_name} s3 cp {src_path} s3://{bucket_name}/{object_key} --no-progress",
         "expected": "Upload successful"},
        marks=pytest.mark.aws,
        id="aws-upload-big"
    ),
    pytest.param(
        {"command": "rclone copy {src_path} {bucket_name}:{object_key} --progress",
         "expected": "Transferred:"},
        marks=pytest.mark.rclone,
        id="rclone-upload-big"
    )
]
@pytest.mark.parametrize(
    "fixture_create_big_file, cmd_template, expected",
    [
        pytest.param(
            size,
            "".join(cmd.values[0]["command"]),
            "".join(cmd.values[0]["expected"]),
            marks=[*cmd.marks],
            id=f"{cmd.id}-{size['size']}{size['unit']}"
        )
        for size, cmd in product(size_list, commands)
    ],
    indirect=['fixture_create_big_file']
)
def test_upload_big_objects(profile_name, fixture_bucket_with_name, fixture_create_big_file, active_mgc_workspace, cmd_template, expected):
    """Test uploading large objects through different CLI tools."""
    bucket_name = fixture_bucket_with_name
    tmp_path, total_size = fixture_create_big_file


    # Formatting the command with the bucket name and other parameters
    cmd = cmd_template.format(
        bucket_name=bucket_name,
        src_path=tmp_path,  # Replace with the actual path to the large file
        object_key=tmp_path,
        profile_name=profile_name  # Replace with the actual profile name if needed
    )

    # Executing the command
    result = execute_subprocess(cmd)

    # Verify command success
    assert result.returncode == 0, (
        f"Command failed with exit code {result.returncode}\n"
        f"Command: {cmd}\n"
        f"Error: {result.stderr}"
    )

    # Verify the expected output in the command result
    assert expected in result.stdout, (
        f"Expected output '{expected}' not found in command output.\n"
        f"Command: {cmd}\n"
        f"Output: {result.stdout}"
    )

# Uploading multiple objects to S3 using different CLI tools
object_quantity = [100, 1000, 10000]

commands = [
    pytest.param(
        {"command": "mgc object-storage objects upload {src_path} --dst {bucket_name}/{object_key} --no-confirm --raw",
         "expected": "Upload successful"},
        marks=pytest.mark.mgc,
        id="mgc-upload-big"
    ),
    pytest.param(
        {"command": "aws --profile {profile_name} s3 cp {src_path} s3://{bucket_name}/{object_key} --no-progress",
         "expected": "Upload successful"},
        marks=pytest.mark.aws,
        id="aws-upload-big"
    ),
    pytest.param(
        {"command": "rclone copy {src_path} {bucket_name}:{object_key} --progress",
         "expected": "Transferred:"},
        marks=pytest.mark.rclone,
        id="rclone-upload-big"
    )
]
@pytest.mark.parametrize(
    "cmd_template, quantity",
    [
        pytest.param(
            "".join(cmd.values[0]["command"]),
            num,
            marks=[*cmd.marks],
            id=f"{cmd.id}-{num}"
        )
        for num, cmd in product(object_quantity, commands)
    ],
)
def test_upload_multiple_object_cli(profile_name, s3_client, fixture_bucket_with_name, fixture_create_small_file, active_mgc_workspace, cmd_template, quantity):
    """
    Test to upload multiple objects to an S3 bucket in parallel based on the wanted quantity
    :param s3_client: pytest.fixture of boto3 s3 client
    :param fixture_bucket_with_name: pytest.fixture to create a bucket with a unique name
    :param file_path: str: path to the file to be uploaded
    :param object_quantity: int: number of objects to be uploaded
    :return: None
    """
    # Local variables
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_prefix = f"test-{quantity}-small-"


    # Uploading objects in parallel
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        objects_keys = [f"{object_prefix}{i}" for i in range(quantity)]

        futures = [
            # Executing the command
            executor.submit(   
                    execute_subprocess,
                    cmd_template.format(
                        bucket_name=bucket_name,
                        src_path=file_path,  # Uploaded file will be the same 
                        object_key=key,
                        profile_name=profile_name
                    )
                ) for key in objects_keys              
        ]

        # Check for errors in the futures
        for future in as_completed(futures):
            result = future.result()
            assert result.returncode == 0, (
                f"Command failed with exit code {result.returncode}\n"
                f"Error: {result.stderr}"
            )

    objects_in_bucket = len(list_all_objects(s3_client, bucket_name))

    # Verify the expected output in the command result
    assert quantity == objects_in_bucket, (
        f"Expected {quantity} successful uploads, but got {objects_in_bucket}"
    )
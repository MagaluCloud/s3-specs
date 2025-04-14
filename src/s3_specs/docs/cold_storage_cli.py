# + {"jupyter": {"source_hidden": true}}
import pytest
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
    pytest.param(
        {
            "mgc": "standard",
            "aws": "STANDARD",
            "rclone": "standard",
            "expected": "STANDARD"
        },
        id="mgc-standard"
    ),

    pytest.param(
        {
            "mgc": "cold",
            "aws": "COLD",
            "rclone": "cold",
            "expected": "COLD_INSTANT"
        },
        id="cold"
    ),

    pytest.param(
        {
            "mgc": "glacier_ir",
            "aws": "GLACIER_IR",
            "rclone": "cold_instant",
            "expected": "COLD_INSTANT"
        },
        id="glacier-ir"
    ),

    pytest.param(
        {
            "mgc": "cold_instant",
            "aws": "COLD_INSTANT",
            "rclone": "cold_instant",
            "expected": "COLD_INSTANT"
        },
        id="cold-instant"
    )
]
acl_list = [
    pytest.param('private', id='acl-private', marks=pytest.mark.acl),
    pytest.param('public-read', id='acl-public-read', marks=pytest.mark.acl),
    pytest.param('public-read-write', id='acl-public-read-write', marks=pytest.mark.acl),
    pytest.param('authenticated-read', id='acl-authenticated-read', marks=pytest.mark.acl)
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
    "fixture_bucket_with_name, storage_class, cmd_template, expected",
    [
        pytest.param(
            "private",  # Fixture needs acl
            storage_class.values[0],  # Fixture needs storage class
            cmd.values[0]["command"],
            storage_class.values[0]['expected'],
            marks=cmd.marks,
            id=f"{storage_class.id}-{cmd.id}"
        ) for storage_class, cmd in product(storage_classes_list, commands)
    ],
    indirect=["fixture_bucket_with_name"]
)
def test_upload_storage_class_cli(s3_client, active_mgc_workspace, profile_name, fixture_bucket_with_name, fixture_create_small_file, storage_class, cmd_template, expected):
    """
    Test upload to cold storage
    """
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_key = os.path.basename(file_path)

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
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Failed to get object from S3"
        assert response['StorageClass'] == expected, f"Expected storage class {storage_class["aws"]} but got {response['StorageClass']}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
        pytest.fail(f"Command execution failed: {e}")


# Commands for uploading multipart cold storage and other classes
commands = [
    pytest.param(
        {
            "command": "mgc object-storage objects upload --src {file_path} --dst {bucket_name}/{object_key}  --storage-class={storage_class_mgc} --chunk-size 8 --no-confirm --raw --output json",
        },
        marks=pytest.mark.mgc,
        id="upload-mgc"
    ),
    pytest.param(
        {
            "command": "aws s3api create-multipart-upload \
                        --bucket {bucket_name} \
                        --key {object_key} \
                        --profile {profile_name} \
                        --storage-class {storage_class_aws}",
        },
        marks=pytest.mark.aws,
        id="upload-aws"
    ),
]

@pytest.mark.parametrize(
    "fixture_bucket_with_name, storage_class, cmd_template, expected",
    [
        pytest.param(
            "private",  # Fixture needs acl
            storage_class.values[0],  # Fixture needs storage class
            cmd.values[0]["command"],
            storage_class.values[0]['expected'],
            marks=cmd.marks,
            id=f"{storage_class.id}-{cmd.id}"
        ) for storage_class, cmd in product(storage_classes_list, commands)
    ],
    indirect=["fixture_bucket_with_name"]
)
def test_multipart_upload_storage_class_cli(s3_client, active_mgc_workspace, profile_name, fixture_bucket_with_name, fixture_create_small_file, storage_class, cmd_template, expected):
    """
    Test upload to cold storage
    """
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_key = os.path.split(file_path)[-1]

    try:
        # Format and execute the command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            file_path=file_path,
            object_key=object_key,
            storage_class_mgc=storage_class["mgc"], # Redundancy need since they have different equivalent arguments
            storage_class_aws=storage_class["aws"],
        )
        result = execute_subprocess(formatted_cmd)
        assert result.returncode == 0, f"Command failed with exit code {result.returncode}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
        pytest.fail(f"Command execution failed: {e}")

def complete_multipart_upload_aws(bucket_name, object_key, profile_name, storage_class, file_path):
    part_etags = []
    part_numbers = []
    """
    Fixture to complete multipart upload for AWS S3
    """
    # Create a temporary file to upload
    file_path = fixture_create_small_file

    # Create a multipart upload
    response = execute_subprocess(
        f"aws s3api create-multipart-upload --bucket {bucket_name} --key {object_key} --profile {profile_name} --storage-class {storage_class['aws']}"
    )

    # Extract the upload ID from the response
    upload_id = response['UploadId']

    # Upload parts of the file
    part_number = 1
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(5 * 1024 * 1024)
            if not data:
                break
            part_response = execute_subprocess(
                f"aws s3api upload-part --bucket {bucket_name} --key {object_key} --part-number {part_number} --body - --upload-id {upload_id} --profile {profile_name}"
            )
            part_number += 1
            part_etag = part_response['ETag']
            part_etags.append(part_etag)
            part_numbers.append(part_number)
            part_number += 1
    # Complete the multipart upload
    complete_response = execute_subprocess(
        f"aws s3api complete-multipart-upload --bucket {bucket_name} --key {object_key} --upload-id {upload_id} --multipart-upload {{'Parts': [{', '.join(part_numbers)}]}} --profile {profile_name}"
    assert complete_response['ETag'] == part_etags[-1], "ETag mismatch after multipart upload"
    return complete_response
    assert complete_response['ResponseMetadata']['HTTPStatusCode'] == 200, "Failed to complete multipart upload"
    assert complete_response['ETag'] == part_etags[-1], "ETag mismatch after multipart upload"
    return complete_response

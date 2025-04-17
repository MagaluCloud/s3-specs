# + {"jupyter": {"source_hidden": true}}
import pytest
import json
import logging
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess, get_different_profile_from_default, fixture_create_big_file
from s3_specs.docs.tools.crud import fixture_bucket_with_name
from s3_specs.docs.tools.cold_storage import verify_storage_class, create_multipart_object_files
import os
import subprocess
from itertools import product
import time
import uuid # For unique object keys

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
    # pytest.param(
    #    {
    #        "command": "mgc object-storage objects list {bucket_name} --no-confirm --raw --output json",
    #    },
    #    marks=pytest.mark.mgc,
    #    id="list-mgc"
    # ),
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
        marks=[pytest.mark.acl, pytest.mark.cli],
    ),
    pytest.param(
       {
           "acl": "public-read",
           "expected_upload": "public-read",
           "expected_head": "public-read",
           "expected_list": "public-read",
       },
       id="acl-public-read",
       marks=[pytest.mark.acl, pytest.mark.cli],
    ),
    pytest.param(
       {
           "acl": "public-read-write",
           "expected_upload": "public-read-write",
           "expected_head": "public-read-write",
           "expected_list": "public-read-write",
       },
       id="acl-public-read-write",
       marks=[pytest.mark.acl, pytest.mark.cli],
    ),
    pytest.param(
       {
           "acl": "authenticated-read",
           "expected_upload": "authenticated-read",
           "expected_head": "authenticated-read",
           "expected_list": "authenticated-read",
       },
       id="acl-authenticated-read",
       marks=[pytest.mark.acl, pytest.mark.cli],
    ),
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


commands = [
  pytest.param(
      "aws s3api list-objects-v2 --profile {profile_name} --bucket {bucket_name} --prefix {object_key} --query \"Contents[?Key=='{object_key}'].StorageClass\"",
      ["COLD_INSTANT", "GLACIER_IR"],
      marks=pytest.mark.aws,
      id="list-object-with-cold-storage-aws"
  ),
  pytest.param(
      "rclone lsjson --metadata '{profile_name}:{bucket_name}/{object_key}'",
      ["COLD_INSTANT", "GLACIER_IR"],
      marks=pytest.mark.rclone,
      id="list-object-with-cold-storage-rclone"
  ),
  pytest.param(
      "mgc os objects list --dst {bucket_name}",
      ["COLD_INSTANT", "GLACIER_IR"],
      marks=pytest.mark.mgc,
      id="list-object-with-cold-storage-mgc"
  ),
]
@pytest.mark.parametrize("cmd_template, expected",
                         commands
                         )
def test_list_object_with_cold_storage_class_cli(s3_client, active_mgc_workspace, bucket_with_one_object, profile_name, cmd_template, expected):
    bucket_name, object_key, _ = bucket_with_one_object
    
    logging.info("Validating default storage class...")

    cmd_template = cmd_template.format(
        profile_name = profile_name,
        bucket_name = bucket_name,
        object_key = object_key
    )
    
    default_output = execute_subprocess(cmd_command=cmd_template)
    default_storage_class = default_output.stdout
    logging.info(f"default storage_class: {default_storage_class}")

    assert default_output.returncode == 0, f"Expected return code 0, but got {default_output.returncode}"

    logging.info(default_storage_class.upper().find("STANDARD"))
    assert default_storage_class.upper().find("STANDARD") != -1, f"Expected STANDARD as storage class, but got {default_storage_class}"
    
    logging.info("Validating cold storage class...")

    s3_client.copy_object(
        Bucket = bucket_name,
        CopySource=f"{bucket_name}/{object_key}",
        Key = object_key,
        StorageClass="GLACIER_IR"
    )

    cmd_template = cmd_template.format(
        profile_name = profile_name,
        bucket_name = bucket_name,
        object_key = object_key
    )

    output = execute_subprocess(cmd_command=cmd_template)
    storage_class = output.stdout
    logging.info(f"storage_class: {storage_class}")

    assert output.returncode == 0, f"Expected return code 0, but got {output.returncode}"

    assert storage_class.find(expected[0]) != -1 or storage_class.find(expected[1]) != -1, f"Expected COLD INSTANT or GLACIER IR as storage class, but got {storage_class}"


commands = [
    pytest.param(
        "aws --profile {profile_name} s3api put-object --bucket {bucket_name} --key {object_key} --storage-class 'GLACIER_IR' --body {file} --metadata 'metadata1=foo,metadata2=bar'",
        marks=pytest.mark.aws,
        id="put-object-with-cold-and-custom-metadata-aws"
    ),
    pytest.param(
        "rclone copyto {file} {profile_name}:{bucket_name}/{object_key} --s3-storage-class 'GLACIER_IR' --header-upload 'X-Amz-Meta-Metadata1: foo' --header-upload 'X-Amz-Meta-Metadata2: bar' -v",
        marks=pytest.mark.rclone,
        id="put-object-with-cold-and-custom-metadata-rclone"
    ),
]
@pytest.mark.parametrize(
        "cmd_template", commands
)
def test_put_object_with_cold_storage_class_and_custom_metadata_cli(s3_client, active_mgc_workspace, profile_name, fixture_bucket_with_name, fixture_create_small_file, cmd_template):
    bucket_name = fixture_bucket_with_name
    file_path = fixture_create_small_file
    object_key = os.path.basename(file_path)

    cmd_template = cmd_template.format(
        bucket_name = bucket_name,
        profile_name = profile_name,
        file = file_path,
        object_key = object_key
    )

    output = execute_subprocess(cmd_template)

    logging.info(f"Output: {output.stdout}")

    assert output.returncode == 0, f"Expected return code 0, but got {output.returncode}"

    response = s3_client.head_object(Bucket=bucket_name, Key=object_key)

    logging.info(f"Head Object Response: {response}")
    metadata_keys = response['Metadata'].keys()
    metadata_values = response['Metadata'].values()
    assert 'metadata1' in metadata_keys and 'metadata2' in metadata_keys, "Expected metadata1 and metadata2 in Metadata response keys"
    assert 'foo' in metadata_values and 'bar' in metadata_values, "Expected foo and bar in Metadata response values"
    

# MULTIPART UPLOAD TEST
aws_multipart_params = [
    pytest.param(
        None,
        "STANDARD",
        id="aws-multipart-default",
    ),
    pytest.param(
        "STANDARD",
        "STANDARD",
        id="aws-multipart-standard",
    ),
    pytest.param(
        "GLACIER_IR",
        "GLACIER_IR",
        id="aws-multipart-glacier_ir",
    ),
]

@pytest.mark.aws
@pytest.mark.parametrize(
                        "storage_class_arg, expected_final_storage_class", 
                         aws_multipart_params,
                         )
def test_aws_multipart_upload_workflow(s3_client, profile_name, fixture_bucket_with_name, create_multipart_object_files, storage_class_arg, expected_final_storage_class):
    """
    Tests the full multipart upload workflow using AWS CLI s3api:
    1. Create multipart upload (with specified storage class).
    2. Upload parts.
    3. List parts.
    4. Complete multipart upload.
    5. Verify the final object's storage class using s3_client.head_object.
    """
    bucket_name = fixture_bucket_with_name
    object_key, file_paths, part_sizes, body = create_multipart_object_files
    upload_id = None

    try:
        # 1. Create Multipart Upload
        create_cmd_parts = [
            f"aws s3api create-multipart-upload",
            f"--profile {profile_name}",
            f"--bucket {bucket_name}",
            f"--key {object_key}"
        ]
        if storage_class_arg:
            create_cmd_parts.append(f"--storage-class '{storage_class_arg}'")
        create_cmd = " ".join(create_cmd_parts)

        output_create = execute_subprocess(create_cmd)
        assert output_create.returncode == 0, f"Failed to create multipart upload. Stderr: {output_create.stderr}"
        try:
            create_result = json.loads(output_create.stdout)
            upload_id = create_result.get('UploadId')
            assert upload_id, "UploadId not found in create-multipart-upload output."
            logging.info(f"Multipart upload created for {object_key} with UploadId: {upload_id}")
        except (json.JSONDecodeError, AssertionError) as e:
             pytest.fail(f"Could not parse UploadId from create output: {e}\nOutput: {output_create.stdout}")

        list_uploads_cmd = f"aws s3api list-multipart-uploads --profile {profile_name} --bucket {bucket_name} --prefix {object_key}"
        output_list_uploads = execute_subprocess(list_uploads_cmd)
        assert output_list_uploads.returncode == 0
        assert upload_id in output_list_uploads.stdout, "Ongoing upload not found in list-multipart-uploads"

        parts_etags = []

        for idx, file_path in enumerate(file_paths):
            part_number = idx+1
            part_size = part_sizes[idx]

            upload_part_cmd = (
                f"aws s3api upload-part --profile {profile_name} --bucket {bucket_name} --key {object_key} "
                f"--upload-id {upload_id} --part-number {part_number} --body {file_path}" # Simplification: upload whole file as part 1
            )
            output_part = execute_subprocess(upload_part_cmd)
            assert output_part.returncode == 0, f"Failed to upload part {part_number}. Stderr: {output_part.stderr}"
            try:
                part_result = json.loads(output_part.stdout)
                etag = part_result.get('ETag')
                assert etag, "ETag not found in upload-part output for part {part_number}."
                parts_etags.append({'PartNumber': part_number, 'ETag': etag})
                logging.info(f"Uploaded part {part_number} for {object_key}, ETag: {etag}")
            except (json.JSONDecodeError, AssertionError) as e:
                pytest.fail(f"Could not parse ETag for part {part_number}: {e}\nOutput: {output_part.stdout}")

        list_parts_cmd = f"aws s3api list-parts --profile {profile_name} --bucket {bucket_name} --key {object_key} --upload-id {upload_id}"
        output_list_parts = execute_subprocess(list_parts_cmd)
        assert output_list_parts.returncode == 0
        try:
            list_parts_result = json.loads(output_list_parts.stdout)
            uploaded_part_numbers = {part['PartNumber'] for part in list_parts_result.get('Parts', [])}
            expected_part_numbers = {p['PartNumber'] for p in parts_etags}
            assert uploaded_part_numbers == expected_part_numbers, \
                f"List parts mismatch. Expected: {expected_part_numbers}, Got: {uploaded_part_numbers}"
            logging.info(f"List parts successful for {object_key}. Parts: {uploaded_part_numbers}")
        except (json.JSONDecodeError, AssertionError) as e:
             pytest.fail(f"Could not parse or verify list-parts output: {e}\nOutput: {output_list_parts.stdout}")


        parts_json_payload = json.dumps({'Parts': parts_etags})
        logging.info(f"Generated Parts JSON Payload: {parts_json_payload}") # Log the JSON content

        parts_file_path = os.path.abspath("parts.json")
        logging.info(f"Attempting to write parts JSON to: {parts_file_path}")

        try:
            with open(parts_file_path, "w") as f:
                 f.write(parts_json_payload)
            logging.info(f"Successfully wrote parts file: {parts_file_path}")

            if not os.path.exists(parts_file_path):
                 pytest.fail(f"Parts file was not created at expected path: {parts_file_path}")
            else:
                 logging.info(f"Parts file confirmed to exist at: {parts_file_path}")


            complete_cmd = (
                f"aws s3api complete-multipart-upload --profile {profile_name} --bucket {bucket_name} "
                f"--key {object_key} --upload-id {upload_id} "
                f"--multipart-upload file://{parts_file_path}"
            )
            logging.info(f"Executing Complete command: {complete_cmd}")
            output_complete = execute_subprocess(complete_cmd)

        except Exception as e:
             logging.error(f"Error during complete multipart upload step: {e}")
             pytest.fail(f"Complete multipart upload failed: {e}")
        finally:
            if os.path.exists(parts_file_path):
                logging.info(f"Cleaning up parts file: {parts_file_path}")
                os.remove(parts_file_path)

        assert output_complete.returncode == 0, f"Complete multipart upload command failed with return code {output_complete.returncode}. Stderr: {output_complete.stderr}"

        try:
            complete_result = json.loads(output_complete.stdout)
            assert complete_result.get('Key') == object_key, "Completed object key mismatch."
            logging.info(f"Multipart upload completed successfully for {object_key}")
        except (json.JSONDecodeError, AssertionError) as e:
            pytest.fail(f"Could not parse or verify complete output: {e}\nOutput: {output_complete.stdout}")

        upload_id = None

        verify_storage_class(s3_client, bucket_name, object_key, expected_final_storage_class)

    finally:
        if upload_id:
            logging.warning(f"Test failed or interrupted. Aborting multipart upload {upload_id} for key {object_key}")
            abort_cmd = f"aws s3api abort-multipart-upload --profile {profile_name} --bucket {bucket_name} --key {object_key} --upload-id {upload_id}"
            execute_subprocess(abort_cmd)


# --- Test Suite: MGC Client Multipart Upload ---

mgc_multipart_params = [
    pytest.param(
        None,
        "STANDARD",
        id="mgc-multipart-default"
    ),
    pytest.param(
        "standard",
        "STANDARD",
        id="mgc-multipart-standard"
    ),
    pytest.param(
        "cold_instant", 
        "GLACIER_IR",
        id="mgc-multipart-cold_instant"
    ),
]

@pytest.mark.mgc
@pytest.mark.parametrize("mgc_storage_class_arg, expected_final_storage_class", 
                         mgc_multipart_params,
                         )
def test_mgc_multipart_upload_workflow(s3_client, active_mgc_workspace, profile_name, create_multipart_object_files, fixture_bucket_with_name, mgc_storage_class_arg, expected_final_storage_class):
    """
    Tests MGC automatic multipart upload for large files:
    1. Set MGC workspace.
    2. Use `mgc object-storage objects upload` with a large file and optional storage class.
    3. Verify the final object's storage class using s3_client.head_object.
    """
    bucket_name = fixture_bucket_with_name
    object_key, file_paths, part_sizes, body = create_multipart_object_files
    file_path = file_paths[0]

    # 2. Upload using MGC CLI
    upload_cmd_parts = [
        f"mgc object-storage objects upload",
        f'--src "{file_path}"',
        f'--dst "{bucket_name}/{object_key}"',
        f"--raw" 
    ]
    if mgc_storage_class_arg:
        upload_cmd_parts.append(f"--storage-class {mgc_storage_class_arg}")

    upload_cmd = " ".join(upload_cmd_parts)

    upload_success = False
    output_upload = execute_subprocess(upload_cmd)
    if output_upload.returncode == 0:
        try:
            upload_response = json.loads(output_upload.stdout)
            logging.info(f"Upload response: {upload_response}")
            logging.info(f"MGC upload command successful")
            upload_success = True
        except json.JSONDecodeError:
            logging.warning(f"MGC upload stdout wasn't valid JSON, but return code was 0. Assuming success.")
            upload_success = True

    assert upload_success, f"MGC upload failed after. Stderr: {output_upload.stderr}"

    verify_storage_class(s3_client, bucket_name, object_key, expected_final_storage_class)


storage_class = [
    pytest.param(
        "STANDARD",
        id="storage-class-standard"
    ),
    pytest.param(
        "GLACIER_IR",
        id="storage-class-glacier_ir"
    ),
]
commands = [
  pytest.param(
      "aws s3api --profile {profile_name} copy-object --bucket {bucket_name} --key {object_key} --storage-class {storage_class} --copy-source {bucket_name}/{object_key}",
      marks=pytest.mark.aws,
      id="list-object-with-cold-storage-aws"
  ),
  pytest.param(
      "rclone settier {storage_class} {profile_name}:{bucket_name}/{object_key} --dump headers",
      marks=pytest.mark.rclone,
      id="list-object-with-cold-storage-rclone"
  ),
]
@pytest.mark.parametrize("cmd_template",
                         commands
                         )
@pytest.mark.parametrize("storage_class_value", storage_class)
def test_change_storage_class_cli(s3_client, profile_name, bucket_with_one_object, cmd_template, storage_class_value):
    bucket_name, object_key, _ = bucket_with_one_object
    cmd_template = cmd_template.format(
        bucket_name = bucket_name,
        object_key = object_key,
        profile_name = profile_name,
        storage_class = storage_class_value
    )

    output = execute_subprocess(cmd_template)

    logging.info(f"Output: {output.stdout}")

    assert output.returncode == 0, f"Expected return code be 0, but got {output.returncode}"

    verify_storage_class(s3_client, bucket_name, object_key, storage_class_value)

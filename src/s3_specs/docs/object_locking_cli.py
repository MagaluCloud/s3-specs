# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
import subprocess
import os
from shlex import split
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess
from s3_specs.docs.tools.crud import upload_object
from s3_specs.docs.tools.versioning import fixture_versioned_bucket, fixture_versioned_bucket_with_one_object

config = "../params/br-se1.yaml"

pytestmark = [pytest.mark.bucket_versioning, pytest.mark.cli]

import json

object_lock_json = json.dumps({
    "ObjectLockEnabled": "Enabled",
    "Rule":{
        "DefaultRetention":{
            "Mode": "COMPLIANCE",
            "Days": 1
        }
    }
})
### Bucket Locking
commands = [
    # pytest.param(
    #     {'command': 'mgc object-storage buckets object-lock set --dst {bucket_name} --days 1 --no-confirm --raw',
    #      'expected': ""},  # Expected output
    #     marks=pytest.mark.mgc,
    #     id="mgc-set-locked"
    # ),
    pytest.param(
        {
            'command': "aws s3api put-object-lock-configuration --profile {profile_name} --bucket {bucket_name} --object-lock-configuration {object_lock_json}" ,
            'expected': ""
        },
        marks=pytest.mark.aws,
        id="aws-set-locked"
    )
]


@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template, expected",
    [
        pytest.param(
            "private",
            cmd.values[0]["command"],
            cmd.values[0]["expected"],
            marks=[*cmd.marks],
            id=f"{cmd.id}-{"private"}"
        )
        for cmd in commands
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
)
def test_set_bucket_lock_cli(s3_client, fixture_versioned_bucket, profile_name, cmd_template, expected):
    """
    Test bucket creation with object lock enabled across multiple CLI tools.
    
    Args:
        s3_client: Boto3 S3 client fixture
        cmd_template: CLI command template
        profile_name: AWS profile name fixture
        tmp_path: Pytest temporary directory fixture
    """
    # Generate unique bucket name
    bucket_name = fixture_versioned_bucket
    
    try:
        # Format and execute create command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_lock_json=object_lock_json
        )
        result = execute_subprocess(formatted_cmd)
        
        # Verify bucket was created with lock
        bucket_lock = s3_client.get_object_lock_configuration(Bucket=bucket_name)
        assert bucket_lock['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled', \
            "Bucket was not created with object lock enabled"
    except subprocess.CalledProcessError as e:
        # Handle command execution error
        print(f"Command failed with error: {e}")
        assert False, f"Command execution failed: {e}"
        
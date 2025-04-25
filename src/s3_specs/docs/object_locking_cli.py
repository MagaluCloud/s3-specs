# + {"jupyter": {"source_hidden": true}}
import pytest
import subprocess
import json
import logging
from datetime import datetime, timedelta
from itertools import product
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess
from s3_specs.docs.tools.crud import upload_object,fixture_bucket_with_name
from s3_specs.docs.tools.versioning import fixture_versioned_bucket, fixture_versioned_bucket_with_one_object
from s3_specs.docs.tools.locking import bucket_with_lock_enabled, fixture_bucket_with_one_object_with_lock

config = "../params/br-se1.yaml"

pytestmark = [pytest.mark.locking, pytest.mark.cli, pytest.mark.homologacao]

## # Bucket locking
object_lock_json = json.dumps({
    "ObjectLockEnabled": "Enabled",
    "Rule":{
        "DefaultRetention":{
            "Mode": "COMPLIANCE",
            "Days": 1
        }
    }
})
## Set Basic Lock - PASS
commands = [
    pytest.param(
        {
            'command': 'mgc object-storage buckets object-lock set --dst {bucket_name} --days=1 --no-confirm --raw',
            'expected': ""
         },  # Expected output
        marks=pytest.mark.mgc,
        id="mgc-set-locked"
    ),
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
    "fixture_versioned_bucket, cmd_template",
    [
        pytest.param(
            "private",
            cmd.values[0]["command"],
            marks=[*cmd.marks],
            id=f"{cmd.id}-{"private"}"
        )
        for cmd in commands
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
)
def test_set_bucket_lock_cli(s3_client, active_mgc_workspace, fixture_versioned_bucket, profile_name, cmd_template):
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

## # Bucket locking with wrong json values (Fail expected)
commands = [
    # Mgc doesnt need any specification on locking type, test for aws
    pytest.param(
        {
            'command': "aws s3api put-object-lock-configuration --profile {profile_name} --bucket {bucket_name} --object-lock-configuration {object_lock_json}" ,
            'expected': "MalformedXML"
        },
        marks=pytest.mark.aws,
        id="aws-malformed-locked"
    )
]
 
@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template, expected",
    [
        pytest.param(
            "private",
            cmd.values[0]["command"],
            cmd.values[0]["expected"],
            marks=[*cmd.marks], # Xfail - Test Should fail
            id=f"{cmd.id}-{"private"}"
        )
        for cmd in commands
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
)
def test_set_bucket_lock_wrong_mode_cli(fixture_versioned_bucket, profile_name, cmd_template, expected):
    """
    Test bucket creation with an invalid object lock mode across multiple CLI tools.
    
    Args:
        s3_client: Boto3 S3 client fixture
        cmd_template: CLI command template
        profile_name: AWS profile name fixture
    """
    # Generate unique bucket name
    bucket_name = fixture_versioned_bucket

    # Create an invalid object lock JSON with a wrong mode
    invalid_object_lock_json = json.dumps({
        "ObjectLockEnabled": "Enabled",
        "Rule": {
            "DefaultRetention": {
                "Mode": "INVALID_MODE",  # Invalid mode
                "Days": 1
            }
        }
    })

    try:
        # Format and execute create command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_lock_json=invalid_object_lock_json
        )
        result = execute_subprocess(cmd_command=formatted_cmd, expected_failure= True)

        # Assert that the command fails and outputs an error
        assert expected in result.stderr, \
            pytest.fail(f"Expected 'InvalidArgument' error, but got: {result.stderr}")
    except subprocess.CalledProcessError as e:
        # Handle command execution error
        print(f"Command failed with error: {e}")
        assert False, pytest.fail(f"Command execution failed: {e}")

## Set lock on bucket without version - (Fail expected)
commands = [
    pytest.param(
        {
            'command': 'mgc object-storage buckets object-lock set --dst {bucket_name} --days=1 --no-confirm --raw',
            'expected': "InvalidBucketState"
         },  # Expected output
        marks=pytest.mark.mgc,
        id="mgc-set-locked"
    ),
    pytest.param(
        {
            'command': "aws s3api put-object-lock-configuration --profile {profile_name} --bucket {bucket_name} --object-lock-configuration {object_lock_json}" ,
            'expected': "InvalidBucketState"
        },
        marks=pytest.mark.aws,
        id="aws-set-locked"
    )
]

@pytest.mark.parametrize(
    "cmd_template, expected",
    [
        pytest.param(
            cmd.values[0]["command"],
            cmd.values[0]["expected"],
            marks=[*cmd.marks], 
            id=f"{cmd.id}"
        )
        for cmd in commands
    ],
)
def test_set_lock_on_not_versioned_bucket_cli(fixture_bucket_with_name, profile_name, cmd_template, expected):
    """
    Test to try and fail on put locking on an unversioned bucket multiple CLI tools.
    
    Args:
        s3_client: Boto3 S3 client fixture
        cmd_template: CLI command template
        profile_name: AWS profile name fixture
        tmp_path: Pytest temporary directory fixture
    """
    
    # Generate unique bucket name
    bucket_name = fixture_bucket_with_name
    
    # Format and execute create command
    formatted_cmd = cmd_template.format(
        bucket_name=bucket_name,
        profile_name=profile_name,
        object_lock_json=object_lock_json
    )
    result = execute_subprocess(formatted_cmd, True)
    
    assert expected in result.stderr, pytest.fail(f"{result.stderr}")

# Put object on bucket without locking (Fail expected)
commands = [
    pytest.param(
        {
            'command': 'mgc object-storage buckets object-lock get --dst {bucket_name} --no-confirm --raw --output json',
            'expected': "Error: bucket missing object lock configuration"
         },  # Expected output
        marks=pytest.mark.mgc,
        id="mgc-set-locked"
    ),
    pytest.param(
        {
            'command': "aws s3api get-object-lock-configuration --profile {profile_name} --bucket {bucket_name}" ,
            'expected': "InvalidRequest"
        },
        marks=pytest.mark.aws,
        id="aws-set-locked"
    )
]

@pytest.mark.parametrize(
    "cmd_template, expected",
    [
        pytest.param(
            cmd.values[0]["command"],
            cmd.values[0]["expected"],
            marks=[*cmd.marks], 
            id=f"{cmd.id}"
        )
        for cmd in commands
    ],
)
def test_get_object_on_bucket_without_locking(fixture_bucket_with_name, active_mgc_workspace, profile_name, cmd_template, expected):
    """
    Test retrieving object lock configuration on a bucket without object lock enabled using multiple CLI tools.
    
    Args:
        fixture_bucket_with_name: Fixture providing a bucket name
        profile_name: AWS profile name fixture
        cmd_template: CLI command template
        expected: Expected output or error
    """
    # Generate unique bucket name
    bucket_name = fixture_bucket_with_name

    try:
        # Format and execute the get command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_lock_json=object_lock_json
        )
        result = execute_subprocess(formatted_cmd, expected_failure=True)

        # Inconsistencia eventual

        # Assert that the expected error is in the result
        assert expected in result.stderr, \
            pytest.fail(f"Expected '{expected}', {result.stderr}")
    except subprocess.CalledProcessError as e:
        # Handle command execution error
        print(f"Command failed with error: {e}")
        assert False, pytest.fail(f"Command execution failed: {e}")
# Unset active bucket locking (PASS)
commands = [
    pytest.param(
        {
            'command': 'mgc object-storage buckets object-lock unset --dst {bucket_name} --no-confirm --raw',
            'expected': ""
         },  # Expected output
        marks=pytest.mark.mgc,
        id="mgc-unset-locked"
    ),
    pytest.param(
        {
            'command': "aws s3api --profile {profile_name} put-object-lock-configuration  --bucket {bucket_name} --object-lock-configuration '{object_lock_json}'" ,
            'expected': ""
        },
        marks=pytest.mark.aws,
        id="aws-unset-locked"
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
            id=f"{cmd.id}-private"
        )
        for cmd in commands
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
) 
# TODO Eventual inconsistency when getting the object locking
def test_unset_bucket_lock_cli(s3_client, active_mgc_workspace,  bucket_with_lock_enabled, profile_name, cmd_template, expected):
    """
    Test unsetting bucket object lock configuration across multiple CLI tools.
    
    Args:
        s3_client: Boto3 S3 client fixture
        fixture_versioned_bucket: Fixture providing a versioned bucket
        profile_name: AWS profile name fixture
        cmd_template: CLI command template
        expected: Expected output or error
    """
    # Generate unique bucket name
    bucket_name = bucket_with_lock_enabled

    try:
        # Format and execute the unset command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_lock_json='{}' # Empty for aws means disabled
        )
        result = execute_subprocess(formatted_cmd)

        # Verify the object lock configuration is removed
        bucket_lock = s3_client.get_object_lock_configuration(Bucket=bucket_name)
        assert bucket_lock['ObjectLockConfiguration']['ObjectLockEnabled'] not in expected, \
            pytest.fail("Bucket lock configuration was not removed successfully")
    except subprocess.CalledProcessError as e:
        # Handle command execution error
        print(f"Command failed with error: {e}")
        assert False, pytest.fail(f"Command execution failed: {e}")

## # Object Locking

# Set object wrong lock date (Should Fail) (aws)
commands = [
    pytest.param(
        {
            'command': "aws s3api put-object-retention --profile {profile_name} --bucket {bucket_name} --key {object_key}  --retention {retention_json} --output json",
            'expected': "Invalid JSON: Extra data"
        },
        marks=pytest.mark.aws,
        id="aws-unset-locked"
    ),
]

invalid_retention_json = [
    json.dumps({ # json with past datetime
        "Mode": "COMPLIANCE",
        "RetainUntilDate": datetime.isoformat(datetime.now().replace(microsecond=0)- timedelta(days=10))
    }),
    json.dumps({ # Json with incorrect format
        "Mode": "COMPLIANCE",
        "RetainUntilDate": f"{datetime.now().replace(microsecond=0)}"
    })
]

@pytest.mark.parametrize(
    "fixture_versioned_bucket, cmd_template, retention_json, expected",
    [
        pytest.param(
            "private",
            cmd.values[0]["command"],
            json,
            cmd.values[0]["expected"],
            marks=[*cmd.marks], 
            id=f"{cmd.id}-private"
        )
        for cmd, json in product(commands, invalid_retention_json)
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
) 
def test_set_wrong_object_lock_date_cli(fixture_versioned_bucket_with_one_object, profile_name, cmd_template, retention_json, expected):
    """
    Test setting object lock configuration on an object across multiple CLI tools.

    Args:
        s3_client: Boto3 S3 client fixture
        bucket_with_lock_enabled: Fixture providing a bucket with lock enabled
        profile_name: AWS profile name fixture
        cmd_template: CLI command template
        expected: Expected output or error
    """
    # Generate unique bucket and object names
    bucket_name, object_key, _ = fixture_versioned_bucket_with_one_object

    try:

        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            object_key=object_key,
            profile_name=profile_name,
            retention_json=retention_json,
        )
        result = execute_subprocess(formatted_cmd, expected_failure=True)

        # Assert that the expected error is in the result
        assert expected in result.stderr, pytest.fail(f"Expected '{expected}' in stderr, but got: {result.stderr}")

    except subprocess.CalledProcessError as e:
        # Errors besides expected
        logging.info(f"Command failed with error: {e}")
        pytest.fail(f"Command execution failed: {e}")

# Set object locking
commands = [
    pytest.param(
        {
            'command':  'mgc object-storage objects object-lock set --dst={bucket_name}/{object_key} --retain-until-date="{time}" --no-confirm --raw',
            'expected': "Object Lock Configuration"
         },  # Expected output
        marks=pytest.mark.mgc,
        id="mgc-unset-locked"
    ),
    pytest.param(
        {
            'command': "aws s3api put-object-retention --profile {profile_name} --bucket {bucket_name} --key {object_key}  --retention {retention_json}" ,
            'expected': "Object Lock Configuration"
        },
        marks=pytest.mark.aws,
        id="aws-unset-locked"
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
            id=f"{cmd.id}-private"
        )
        for cmd in commands
    ],
    # versioned_bucket_with_one_object depends on fixture_versioned_bucket which asks for values
    indirect=['fixture_versioned_bucket']
)
def test_set_object_lock_unlocked_bucket_cli(s3_client, active_mgc_workspace, fixture_versioned_bucket_with_one_object, profile_name, cmd_template, expected):
    """
    Test setting object lock configuration on an object across multiple CLI tools.

    Args:
        s3_client: Boto3 S3 client fixture
        bucket_with_lock_enabled: Fixture providing a bucket with lock enabled
        profile_name: AWS profile name fixture
        cmd_template: CLI command template
        expected: Expected output or error
    """
    # Generate unique bucket and object names
    bucket_name, object_key, _ = fixture_versioned_bucket_with_one_object
    time = datetime.isoformat(datetime.now().replace(microsecond=0) + timedelta(seconds=10))

    # Format and execute the set object lock command
    retention_json = json.dumps({
        "Mode": "COMPLIANCE",
        "RetainUntilDate": time
    })

    try:
        # Format and execute the set object lock command
        formatted_cmd = cmd_template.format(
            bucket_name=bucket_name,
            object_key=object_key,
            profile_name=profile_name,
            retention_json=retention_json,
            time=time
        )
        result = execute_subprocess(formatted_cmd, True)
        assert expected in result.stderr, \
            pytest.fail(f"Expected '{expected}' in stdout, but got: {result.stderr}")

    except subprocess.CalledProcessError as e:
        # Handle command execution error
        print(f"Command failed with error: {e}")
        assert False, pytest.fail(f"Command execution failed: {e}")
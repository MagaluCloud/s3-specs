import pytest
import shlex
import logging
import subprocess
from time import sleep

from s3_specs.docs.s3_helpers import wait_until_bucket_is_empty

pytestmark = [pytest.mark.homologacao, pytest.mark.cli, pytest.mark.quick, pytest.mark.basic]

commands = [
    pytest.param(
        "aws --profile {profile_name} s3 rm s3://{bucket_name} --recursive",
        id="delete-all-aws",
        marks=pytest.mark.aws
    ),
    pytest.param(
        "mgc object-storage objects delete-all {bucket_name} --no-confirm",
        id="delete-all-mgc",
        marks=pytest.mark.mgc
    ),
    pytest.param(
        "rclone delete {profile_name}:{bucket_name}",
        id="delete-all-rclone",
        marks=pytest.mark.rclone
    ),
]

@pytest.mark.parametrize(
    "cmd_template", commands
)
def test_delete_all(s3_client, bucket_with_many_objects, active_mgc_workspace, cmd_template, profile_name):
    bucket_name, _, _, object_key_list = bucket_with_many_objects
    num_objects = len(object_key_list)

    cmd = cmd_template.format(
        bucket_name=bucket_name, 
        profile_name=profile_name
    )

    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True)    

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd}: {result.stdout}")

    is_empty = wait_until_bucket_is_empty(s3_client=s3_client, bucket_name=bucket_name, max_retries=1, delay=1)

    assert is_empty, "Expected bucket being empty"
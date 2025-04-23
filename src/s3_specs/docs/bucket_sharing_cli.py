# ---
# jupyter:
#   jupytext:
#     cell_metadata_json: true
#     notebook_metadata_filter: language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.12.7
# ---

# # Presigned URL Tests
#
# Testes relacionados à criação e uso de URLs pré-assinados para buckets com diferentes operações e configurações



# + {"jupyter": {"source_hidden": true}}
import pytest
import os
import logging
import subprocess
import json
import boto3
from pathlib import Path
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.utils import fixture_create_small_file, execute_subprocess, get_different_profile_from_default, fixture_create_big_file
from s3_specs.docs.tools.crud import fixture_bucket_with_one_object
import requests
import re

# + {"tags": ["parameters"]}[
pytestmark = [pytest.mark.bucket_sharing, pytest.mark.presign]
config = "../params/br-ne1.yaml"


# -

# ### Teste 1: Criar URL Pré-assinada (GET)
#
# Este teste verifica a criação de URLs pré-assinadas para operações GET.

# +

operations = [
    pytest.param(
        "GET",
        id="get"
    ),
    pytest.param(
        "PUT",
        id="put"
    )
]
commands = [
    pytest.param(
        {
            "command": "mgc os objects presign --dst {bucket_name}/{object_key} --method {operation} --no-confirm --raw",
            "expected": "url"
        },
        marks = pytest.mark.mgc,
        id="mgc-presign"
    ),
    pytest.param(
        {
            "command": "aws s3 presign {bucket_name}/{object_key} --profile {profile_name}",
            "expected": "url"
        },
        marks = pytest.mark.aws,
        id="aws-presign"
    ),
]

@pytest.mark.parametrize("operation", operations)
@pytest.mark.parametrize("command", commands)
def test_generate_presigned_url_cli(
    active_mgc_workspace,
    profile_name,
    fixture_bucket_with_one_object,
    command,
    operation
    ):
    """
    Test the generation and usage of presigned URLs via CLI commands.

    Parameters:
    - active_mgc_workspace: Active workspace for MGC.
    - profile_name: AWS profile name.
    - fixture_bucket_with_one_object: Fixture providing a bucket and an object.
    - command: CLI command template for generating presigned URLs.
    - operation: S3 operation (e.g., GET, PUT).
    """

    bucket_name, object_key = fixture_bucket_with_one_object

    # Testing presigned URL generation
    try:
        # Format and execute the command
        formatted_cmd = command['command'].format(
            bucket_name=bucket_name,
            profile_name=profile_name,
            object_key=object_key,
            operation=operation,
        )
        result = execute_subprocess(formatted_cmd)
        logging.info(f"Presigned URL generated: {result}")

        # Validate the presigned URL
        assert result is not None, "Presigned URL was not generated"

        # Extract the URL from the command output
        match = re.search(r"(?P<url>https?://[^\s]+)", result.stdout)
        assert match, "No valid URL found in the command output"
        presigned_url = match.group("url")

        # Log the URL for debugging purposes
        logging.info("Validating access via presigned URL")
        
        # Perform a GET request to the presigned URL
        response = requests.get(presigned_url)
        logging.info(f"Request: {response}")
        # Validate the response status and content
        assert response.status_code == 200, f"Failed to access presigned URL: Status {response.status_code}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
        pytest.fail(f"Command execution failed: {e}")

run_example(__name__, "test_generate_presigned_url_cli", config=config)

# ## Referências
#
# - [Boto3 S3 generate_presigned_url](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url)
# - [AWS S3 Presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-urls.html)
# - [Boto3 S3 put_bucket_acl](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_acl)
# - [AWS S3 ACL Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html)
# - [Python requests](https://docs.python-requests.org/en/latest/)
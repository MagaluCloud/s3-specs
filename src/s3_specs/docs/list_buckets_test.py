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

# # List buckets
#
# Lista os buckets de um perfil[<sup>1</sup>](../glossary#profile)


# + {"tags": ["parameters"]}
config = "../params/br-ne1.yaml"

# + {"jupyter": {"source_hidden": true}}
import pytest
import random
import os
import logging
import subprocess
from shlex import split, quote
from s3_specs.docs.s3_helpers import run_example

pytestmark = [pytest.mark.basic, pytest.mark.quick, pytest.mark.homologacao]
config = os.getenv("CONFIG", config)
# -

# ## Exemplos
#
# ### Boto3
#
# O comando para listar buckets no boto3 é o `list_buckets`.

# +
@pytest.mark.skip("This is a very expensive operation that may result in timeout")
def test_list_buckets(s3_client):
    response = s3_client.list_buckets(MaxBuckets=100)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful bucket list."
    buckets = response.get('Buckets')
    assert isinstance(buckets, list), "Expected 'Buckets' to be a list."
    buckets_count = len(buckets)
    assert isinstance(buckets_count, int), "Expected buckets count to be an integer."
    logging.info(f"Bucket list returned with status {response_status} and a list of {buckets_count} buckets")

    if buckets_count > 0:
        bucket_name = random.choice(buckets).get('Name')
        assert isinstance(bucket_name, str) and bucket_name, "Expected bucket name to be a non-empty string."
        logging.info(f"One of those buckets is named {random.choice(buckets).get('Name')}")

run_example(__name__, "test_boto_list_buckets", config=config)


# ## Referências
#
# - [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)

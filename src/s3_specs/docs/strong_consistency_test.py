import pytest
import logging
from s3_specs.docs.s3_helpers import run_example
import os
import secrets
import botocore


from s3_specs.docs.object_basic_test import test_list_objects

@pytest.fixture
def bucket_with_many_objects(s3_client):
    bucket_name = "test-s3specs-strong"
    object_prefix = "test-object"
    content = secrets.token_bytes(512) 
    object_key_list = []
    logging.info(f"Using bucket_name={bucket_name}, content size={len(content)}, object_prefix={object_prefix}")

    for i in range(5):
        object_key = f"{object_prefix}-{i}.txt"
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
        object_key_list.append(object_key)

    yield bucket_name, object_prefix, content, object_key_list

    for object_key in object_key_list:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)


test_list_object = test_list_objects
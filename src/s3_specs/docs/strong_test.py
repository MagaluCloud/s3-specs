import pytest
import logging
import secrets
import os

from s3_specs.docs.object_basic_test import (
    test_list_objects,
    test_head_object,
    test_put_object as _test_put_object,
    test_delete_object,
    test_get_object,
)
from s3_specs.docs.bucket_basic_test import test_list_all_buckets
from s3_specs.docs.big_objects_test import test_multipart_download as _test_multipart_download


@pytest.fixture
def bucket_name(default_profile):
    if "bucket" not in default_profile:
        pytest.skip(f"Profile '{default_profile.get('profile_name', 'unknown')}' doesn't have a 'bucket' configured.")
    
    return default_profile["bucket"]


@pytest.fixture
def bucket_with_many_objects(s3_client, bucket_name):
    object_prefix = f"test-object-{secrets.token_hex(4)}"
    content = secrets.token_bytes(512)
    object_key_list = []

    logging.info(f"Using bucket={bucket_name}, prefix={object_prefix}")

    for i in range(5):
        object_key = f"{object_prefix}-{i}.txt"
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
        object_key_list.append(object_key)

    yield bucket_name, object_prefix, content, object_key_list

    for object_key in object_key_list:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        except Exception as e:
            logging.warning(f"Failed to delete {object_key}: {e}")


@pytest.fixture
def bucket_with_one_object(s3_client, bucket_name):
    object_key = f"test-single-{secrets.token_hex(8)}.txt"
    content = secrets.token_bytes(512)

    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)

    yield bucket_name, object_key, content

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    except Exception as e:
        logging.warning(f"Failed to delete {object_key}: {e}")


@pytest.fixture
def existing_bucket_name(bucket_name):
    return bucket_name


# Direct aliases for tests that don't need modification
test_list_all_buckets = test_list_all_buckets
test_get_object = test_get_object
test_delete_object = test_delete_object
test_head_object = test_head_object
test_list_object = test_list_objects


put_object_key_names = [
    "simple_name.txt",
    "name/that/looks/like/a/path.foo",
    "name_with_acentuação and emojis👀👀👀 and spaces.jpg",
    secrets.token_hex(462),
]

@pytest.mark.parametrize('object_key', put_object_key_names, ids=range(len(put_object_key_names)))
def test_put_object(s3_client, existing_bucket_name, object_key):
    try:
        return _test_put_object(s3_client, existing_bucket_name, object_key)
    finally:
        try:
            s3_client.delete_object(Bucket=existing_bucket_name, Key=object_key)
        except Exception as e:
            logging.warning(f"Failed to cleanup {object_key}: {e}")


@pytest.fixture
def fixture_create_big_file(tmp_path, request, s3_client, bucket_with_many_objects):
    bucket_name, *_ = bucket_with_many_objects
    size_info = request.param
    size = size_info["size"]
    unit = size_info["unit"]

    factor = {"mb": 1024**2, "gb": 1024**3}[unit.lower()]
    total_size = size * factor

    file_path = tmp_path / f"bigfile_{size}{unit}_{secrets.token_hex(4)}.bin"
    
    with open(file_path, "wb") as f:
        f.write(secrets.token_bytes(total_size))

    object_key = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)

    yield str(file_path), total_size

    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    except Exception as e:
        logging.warning(f"Failed to delete {object_key}: {e}")


size_list = [
    {"size": 10, "unit": "mb"},
    {"size": 20, "unit": "mb"},
    {"size": 50, "unit": "mb"},
]
ids_list = [f"{s['size']}{s['unit']}" for s in size_list]

@pytest.mark.parametrize("fixture_create_big_file", size_list, ids=ids_list, indirect=True)
@pytest.mark.slow
@pytest.mark.big_objects
def test_multipart_download(s3_client, bucket_with_many_objects, fixture_create_big_file):
    bucket_name = bucket_with_many_objects[0]
    return _test_multipart_download(s3_client, bucket_name, fixture_create_big_file)
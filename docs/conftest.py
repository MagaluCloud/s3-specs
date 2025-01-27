import os
import boto3
import pytest
import time
import yaml
import logging
import subprocess
import shutil
import botocore.exceptions

from s3_helpers import (
    generate_unique_bucket_name,
    delete_bucket_and_wait,
    create_bucket_and_wait,
    delete_object_and_wait,
    put_object_and_wait,
    cleanup_old_buckets,
    get_spec_path,
    change_policies_json,
    delete_policy_and_bucket_and_wait,
    get_tenants,
)
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


def pytest_addoption(parser):
    parser.addoption("--config", action="store", help="Path to the YAML config file")

@pytest.fixture
def test_params(request):
    """
    Loads test parameters from a config file or environment variable.
    """
    config_path = request.config.getoption("--config") or os.environ.get("CONFIG_PATH", "../params.example.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

@pytest.fixture
def default_profile(test_params):
    """
    Returns the default profile from test parameters.
    """
    return test_params["profiles"][test_params.get("default_profile_index", 0)]

@pytest.fixture
def lock_mode(default_profile):
    return default_profile.get("lock_mode", "COMPLIANCE")

@pytest.fixture
def profile_name(default_profile):
    return (
        default_profile.get("profile_name")
        if default_profile.get("profile_name")
        else pytest.skip("This test requires a profile name")
    )

@pytest.fixture
def mgc_path(default_profile):
    """
    Validates and returns the path to the 'mgc' binary.
    """
    mgc_path_field_name = "mgc_path"
    if not default_profile.get(mgc_path_field_name):
        path = shutil.which("mgc")
    else:
        spec_dir = os.path.dirname(get_spec_path())
        path = os.path.join(spec_dir, default_profile.get(mgc_path_field_name))
    if not os.path.isfile(path):
        pytest.fail(f"The specified mgc_path '{path}' (absolute: {os.path.abspath(path)}) does not exist or is not a file.")
    return path

@pytest.fixture
def active_mgc_workspace(profile_name, mgc_path):
    # set the profile
    result = subprocess.run([mgc_path, "workspace", "set", profile_name],
                            capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("This test requires an mgc profile name")

    logging.info(f"mcg workspace set stdout: {result.stdout}")
    return profile_name

@pytest.fixture
def s3_client(default_profile):
    """
    Creates a boto3 S3 client using profile credentials or explicit config.
    """
    if "profile_name" in default_profile:
        session = boto3.Session(profile_name=default_profile["profile_name"])
    else:
        session = boto3.Session(
            region_name=default_profile["region_name"],
            aws_access_key_id=default_profile["aws_access_key_id"],
            aws_secret_access_key=default_profile["aws_secret_access_key"],
        )
    return session.client("s3", endpoint_url=default_profile.get("endpoint_url"))

@pytest.fixture
def bucket_name(request, s3_client):
    test_name = request.node.name.replace("_", "-")
    unique_name = generate_unique_bucket_name(base_name=f"{test_name}")

    # Yield the bucket name for the test to use
    yield unique_name

    # Teardown: delete the bucket after the test
    delete_bucket_and_wait(s3_client, unique_name)

@pytest.fixture
def existing_bucket_name(s3_client):
    # Generate a unique name for the bucket to simulate an existing bucket
    bucket_name = generate_unique_bucket_name(base_name="existing-bucket")

    # Ensure the bucket exists, creating it if necessary
    create_bucket_and_wait(s3_client, bucket_name)

    # Yield the existing bucket name to the test
    yield bucket_name
    
    objects = s3_client.list_objects(Bucket=bucket_name).get("Contents", [])

    if objects:
        for obj in objects:
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    
    # Teardown: delete the bucket after the test
    delete_bucket_and_wait(s3_client, bucket_name)
    
@pytest.fixture
def create_multipart_object_files():
    object_key = "multipart_file.txt"

    body = b"A" * 10 * 1024 * 514  # 50 MB

    # Dividindo o dado em 2 partes
    total_size = len(body)  # Tamanho total em bytes
    part_sizes = [total_size, 1]
    # Criar as partes diretamente em memória
    part_bytes = []
    start = 0
    for size in part_sizes:
        part_bytes.append(body[start:start + size])
        start += size
    
    yield object_key, body, part_bytes

@pytest.fixture
def create_big_file_with_two_parts():
    object_key = "large_file.txt"
    with open(object_key, "w") as f:
        f.write("A" * 10 * 1024 * 1024 * 5)

    # Dividindo o arquivo em 2 partes
    total_size = 10 * 1024 * 1024 * 5  # Tamanho total em bytes (50 MB)
    part_sizes = [total_size // 2] * 2  # Cada parte terá metade do tamanho

    # Se o tamanho total não for divisível por 2, ajusta a última parte
    part_sizes[-1] += total_size % 2

    part_files = []
    with open(object_key, "r") as f:
        for i, size in enumerate(part_sizes):
            part_path = f"{object_key.split('.')[0]}_part_{i+1}.txt"
            with open(part_path, "w") as part_file:
                part_file.write(f.read(size))
            part_files.append(part_path)
    
    yield object_key, part_files

    os.remove(object_key)

    for i in range(2):
        os.remove(f"{object_key.split('.')[0]}_part_{i+1}.txt")


@pytest.fixture
def bucket_with_one_object_and_cold_storage_class(s3_client):
    # Generate a unique bucket name and ensure it exists
    bucket_name = generate_unique_bucket_name(base_name="fixture-bucket")
    create_bucket_and_wait(s3_client, bucket_name)

    # Define the object key and content, then upload the object
    object_key = "test-object.txt"
    content = b"Sample content for testing presigned URLs."
    put_object_and_wait(s3_client, bucket_name, object_key, content, storage_class="GLACIER_IR")

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Delete the object and bucket after the test
    delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)
@pytest.fixture
def bucket_with_one_object(s3_client):
    # Generate a unique bucket name and ensure it exists
    bucket_name = generate_unique_bucket_name(base_name="fixture-bucket")
    create_bucket_and_wait(s3_client, bucket_name)

    # Define the object key and content, then upload the object
    object_key = "test-object.txt"
    content = b"Sample content for testing presigned URLs."
    put_object_and_wait(s3_client, bucket_name, object_key, content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Tente deletar o objeto e o bucket, mas ignore o erro NoSuchBucket
    try:
        delete_object_and_wait(s3_client, bucket_name, object_key)
    except botocore.exceptions.ClientError as e:
        if "NoSuchBucket" in str(e):  # Ignora o erro se o bucket não existir mais
            pass
        else:
            raise e  # Se for outro erro, levante a exceção normalmente

    try:
        delete_bucket_and_wait(s3_client, bucket_name)
    except botocore.exceptions.ClientError as e:
        if "NoSuchBucket" in str(e):  # Ignora o erro se o bucket não existir mais
            pass
        else:
            raise e  # Se for outro erro, levante a exceção normalmente

@pytest.fixture
def bucket_with_one_storage_class_cold_object(s3_client, bucket_with_one_object):
    # Generate a unique bucket name and ensure it exists
    bucket_name, object_key, content = bucket_with_one_object

    s3_client.copy_object(
        Bucket = bucket_name,
        CopySource=f"{bucket_name}/{object_key}",
        Key = object_key,
        StorageClass="GLACIER_IR"
    )

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content


@pytest.fixture
def versioned_bucket_with_one_object(s3_client, lock_mode):
    """
    Fixture to create a versioned bucket with one object for testing.
    
    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode for the bucket or objects (e.g., 'GOVERNANCE', 'COMPLIANCE')
    :return: Tuple containing bucket name, object key, and object version ID
    """
    base_name = "versioned-bucket-with-one-object"
    bucket_name = generate_unique_bucket_name(base_name=base_name)

    # Create bucket and enable versioning
    create_bucket_and_wait(s3_client, bucket_name)
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )

    # Upload a single object and get it's version
    object_key = "test-object.txt"
    content = b"Sample content for testing versioned object."
    object_version = put_object_and_wait(s3_client, bucket_name, object_key, content)

    # Yield details to tests
    yield bucket_name, object_key, object_version

    # Cleanup
    try:
        cleanup_old_buckets(s3_client, base_name, lock_mode)
    except Exception as e:
        print(f"Cleanup error {e}")

@pytest.fixture
def bucket_with_one_object_and_lock_enabled(s3_client, lock_mode, versioned_bucket_with_one_object):
    bucket_name, object_key, object_version = versioned_bucket_with_one_object
    # Enable bucket lock configuration if not already set
    s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            'ObjectLockEnabled': 'Enabled',
        }
    )
    logging.info(f"Object lock configuration enabled for bucket: {bucket_name}")

    # Yield details to tests
    yield bucket_name, object_key, object_version

@pytest.fixture
def lockeable_bucket_name(s3_client, lock_mode):
    """
    Fixture to create a versioned bucket for tests that will set default bucket object-lock configurations.

    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode ('GOVERNANCE', 'COMPLIANCE', or None)
    :return: The name of the created bucket
    """
    base_name = "lockeable-bucket"

    # Generate a unique name and create a versioned bucket
    bucket_name = generate_unique_bucket_name(base_name=base_name)
    create_bucket_and_wait(s3_client, bucket_name)
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )

    logging.info(f"Created versioned bucket: {bucket_name}")

    # Yield the bucket name for tests
    yield bucket_name

    # Cleanup after tests
    try:
        cleanup_old_buckets(s3_client, base_name, lock_mode)
    except Exception as e:
        logging.error(f"Cleanup error for bucket '{bucket_name}': {e}")

@pytest.fixture
def bucket_with_lock(lockeable_bucket_name, s3_client, lock_mode):
    """
    Fixture to create a bucket with Object Lock and a default retention configuration.

    :param lockeable_bucket_name: Name of the lockable bucket.
    :param s3_client: Boto3 S3 client.
    :param lock_mode: Lock mode ('GOVERNANCE' or 'COMPLIANCE').
    :return: The name of the bucket with Object Lock enabled.
    """
    bucket_name = lockeable_bucket_name

    # Enable Object Lock configuration with a default retention rule
    retention_days = 1
    s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": lock_mode,
                    "Days": retention_days
                }
            }
        }
    )

    logging.info(f"Bucket '{bucket_name}' configured with Object Lock and default retention.")

    return bucket_name

@pytest.fixture
def bucket_with_lock_and_object(s3_client, bucket_with_lock):
    """
    Prepares an S3 bucket with object locking enabled and uploads a dynamically
    generated object with versioning.

    :param s3_client: boto3 S3 client fixture.
    :param bucket_with_lock: Name of the bucket with versioning and object locking enabled.
    :return: Tuple of (bucket_name, object_key, object_version).
    """
    bucket_name = bucket_with_lock
    object_key = "test-object.txt"
    object_content = "This is a dynamically generated object for testing."

    # Upload the generated object to the bucket
    response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=object_content)
    object_version = response.get("VersionId")

    # Verify that the object is uploaded and has a version ID
    if not object_version:
        pytest.fail("Uploaded object does not have a version ID")

    # Return bucket name, object key, and version ID
    return bucket_name, object_key, object_version
    
@pytest.fixture
def bucket_with_one_object_policy(multiple_s3_clients, request):
    """
    Prepares an S3 bucket with object and defines its object policies.

    :param s3_client: boto3 S3 client fixture.
    :param existing_bucket_name: Name of the bucket after its creating on the fixture of same name.
    :param request: dictionary of policy expecting the helper function change_policies_json.
    :return: bucket_name.
    """
        
    client = multiple_s3_clients[0]
        
    # Generate a unique name and create a versioned bucket
    base_name = "policy-bucket"
    object_key = "PolicyObject.txt"
    bucket_name = generate_unique_bucket_name(base_name=base_name)
    
    create_bucket_and_wait(client, bucket_name)
    put_object_and_wait(client, bucket_name, object_key, "42")    
    
    tenants = get_tenants(multiple_s3_clients)
    
    policy = change_policies_json(bucket=bucket_name, policy_args=request.param, tenants=tenants)
    client.put_bucket_policy(Bucket=bucket_name, Policy = policy)
    
    # Yield the bucket name and object key to the test
    yield bucket_name, object_key
    
    # Teardown: delete the bucket after the test
    delete_policy_and_bucket_and_wait(client, bucket_name, request)




@pytest.fixture
def multiple_s3_clients(request, test_params):
    """
    Creates multiple S3 clients based on the profiles provided in the test parameters.

    :param test_params: dictionary containing the profiles names.
    :param request: dictionary that have number_clients int.
    :return: A list of boto3 S3 client instances.
    """
    number_clients = request.param["number_clients"]
    clients = [p for p in test_params["profiles"][:number_clients]]
    sessions = []
    
    
    for client in clients:
        if "profile_name" in client:
            session = boto3.Session(profile_name=client["profile_name"])
        else:
            session = boto3.Session(
                region_name=client["region_name"],
                aws_access_key_id=client["aws_access_key_id"],
                aws_secret_access_key=client["aws_secret_access_key"],
            )
        sessions.append(session.client("s3", endpoint_url=client.get("endpoint_url")))
        
    return sessions
    
    

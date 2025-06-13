import os
import boto3
import pytest
import time
import yaml
import logging
import subprocess
import shutil
from s3_specs.docs.tools.utils import get_clients
from s3_specs.docs.s3_helpers import (
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
    replace_failed_put_without_version,
    put_object_lock_configuration_with_determination,
    get_policy_with_determination,
    probe_versioning_status,
    delete_all_objects_and_wait,
    delete_all_objects_with_version_and_wait,
)
from s3_specs.docs.utils.consistency import (
    setup_standard_bucket,
    setup_versioned_bucket,
    resolve_bucket,
    available_buckets
)
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


def pytest_addoption(parser):
    parser.addoption("--config", action="store", help="Path to the YAML config file")
    parser.addoption("--profile", action="store", help="profile to use for the tests")
    parser.addoption("--run-dev", action="store_true", help="Rodar testes no modo dev")
    parser.addoption("--manual-standard", action="store", default=None, help="Bucket padrão (não versionado) manual")
    parser.addoption("--manual-versioned", action="store", default=None, help="Bucket versionado manual")


@pytest.fixture(autouse=True)
def skip_based_on_region_marker(s3_client, request):
    marker = request.node.get_closest_marker("only_run_in_region")
    if marker:
        regions_to_run = []
        if marker.args:
            regions_to_run.extend(marker.args)
        if not regions_to_run:
            logging.warning("Marcador 'skip_in_region' usado sem especificar regiões.")
            return 
        current_region = s3_client.meta.region_name

        logging.info(f"\n[Fixture skip_based_on_region_marker] Teste: {request.node.name}")
        logging.info(f"  Marcador 'only_run_in_region' encontrado com regiões: {regions_to_run}")

        if current_region not in regions_to_run:
            skip_message = f"Teste pulado porque a região do cliente não está na lista de skip do marcador {regions_to_run}"
            logging.info(f"{skip_message}")
            pytest.skip(skip_message)
        else:
            logging.info("Região atual está na lista de regiões onde o teste pode ser executado.")

@pytest.fixture(autouse=True)
def skip_if_is_dev_run(s3_client, request):
    marker = request.node.get_closest_marker("skip_if_dev")
    isDevRun = request.config.getoption("--run-dev")
    if marker:
        if isDevRun:
            pytest.skip("This test doesn't working with dev mode")

@pytest.fixture(scope="session", autouse=True)
def verify_credentials(get_clients, request):
    tenants = get_tenants(get_clients)
    isDevRun = request.config.getoption("--run-dev")

    if isDevRun:
        return

    if not len(tenants) == len(set(tenants)) or len(tenants) < 2:
        pytest.exit("Perfis estão configurados de forma incorreta. É necessário que tenha dois perfis configurados para diferentes owners")

@pytest.fixture
def test_params(request):
    """
    Loads test parameters from a config file or environment variable.
    """
    config_path = request.config.getoption("--config") or os.environ.get("CONFIG_PATH", "../params.example.yaml")
    
    profile = request.config.getoption("--profile") or os.environ.get("PROFILE", None)
    logging.info(f"Profile: {profile}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        if not profile:
            return config
        sufix = ["", "-second", "-sa"]
        for index, profile_index in enumerate(config["profiles"]):
            if "profile_name" in profile_index and index < 3:
                profile_index["profile_name"] = f"{profile}{sufix[index]}"
        
        return config


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
def policy_wait_time(default_profile):
    return default_profile.get("policy_wait_time", 0)

@pytest.fixture
def lock_wait_time(default_profile):
    return default_profile.get("lock_wait_time", 0)

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
        raise Exception(f"Failed setting correct profile for mgc cli: {result.stderr}")

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
def rbac_s3_client(test_params, request):
    """
    Creates a boto3 S3 client using profile credentials or explicit config.
    RBAC clients only
    """

    index = request.param # singular int

    def get_client(profile):
        session = boto3.Session(
            region_name=profile["region_name"],
            aws_access_key_id=profile["aws_access_key_id"],
            aws_secret_access_key=profile["aws_secret_access_key"],
        )
        return session.client("s3", endpoint_url=profile.get("endpoint_url"))

    rbac_profiles = [
        get_client(profile)
        for profile in test_params['profiles']
        if 'rbac' in profile.get('profile_name', '')
    ]
    return rbac_profiles[index]

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

    # Teardown
    try:
        # Remove policy if present
        response = s3_client.delete_bucket_policy(Bucket=bucket_name) 
        logging.info(f"delete_bucket_policy response:{response}")

        delete_all_objects_and_wait(s3_client, bucket_name)
        delete_bucket_and_wait(s3_client, bucket_name)
    except Exception as e:
        logging.error(f"existing_bucket_name teardown failed: {e}")
    
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

@pytest.fixture(params=[{
'object_key': 'test-object.txt'
}])
def bucket_with_one_object(request, s3_client):
    # this fixture accepts an optional request.param['object_key'] if you
    # need a custom specific key name for your test
    object_key = request.param['object_key']

    # Generate a unique bucket name and ensure it exists
    bucket_name = generate_unique_bucket_name(base_name="test-fixture-bucket")
    create_bucket_and_wait(s3_client, bucket_name)

    # Define the object content, then upload the object
    content = b"Sample content for testing presigned URLs."
    put_object_and_wait(s3_client, bucket_name, object_key, content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Delete the object and bucket after the test
    delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)

@pytest.fixture(params=[{
    'object_prefix': "",
    'object_key_list': ['test-object-1.txt', 'test-object-2.txt']
}])
def bucket_with_many_objects(request, s3_client):
    # this fixture accepts an optional request.param['object_key_list'] with a list of custom key names
    object_key_list = request.param['object_key_list']
    # and a string prefix to prepend on all objects
    object_prefix = request.param.get('object_prefix', "")

    bucket_name = generate_unique_bucket_name(base_name='fixture-bucket-with-many-objects')
    create_bucket_and_wait(s3_client, bucket_name)

    content = b"Sample content for testing presigned URLs."
    for object_key in object_key_list:
        put_object_and_wait(s3_client, bucket_name, f"{object_prefix}{object_key}", content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_prefix, content, object_key_list

    # Teardown: Delete the object and bucket after the test
    for object_key in object_key_list:
        delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)


@pytest.fixture(params=[{
    'object_prefix': "",
    'object_key_list': ['test-object-1.txt', 'test-object-2.txt']
}],
scope="session"
)
def bucket_with_many_objects_session(request, s3_client):
    # this fixture accepts an optional request.param['object_key_list'] with a list of custom key names
    object_key_list = request.param['object_key_list']
    # and a string prefix to prepend on all objects
    object_prefix = request.param.get('object_prefix', "")

    bucket_name = generate_unique_bucket_name(base_name='fixture-bucket-with-many-objects')
    create_bucket_and_wait(s3_client, bucket_name)

    content = b"Sample content for testing presigned URLs."
    for object_key in object_key_list:
        put_object_and_wait(s3_client, bucket_name, f"{object_prefix}{object_key}", content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_prefix, content, object_key_list

    logging.info(f"object keys list: {object_key_list}")
    # Teardown: Delete the object and bucket after the test
    for object_key in object_key_list:
        delete_object_and_wait(s3_client, bucket_name, object_key)
    delete_bucket_and_wait(s3_client, bucket_name)


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
    start_time = datetime.now()
    base_name = "versioned-bucket-with-one-object"
    bucket_name = generate_unique_bucket_name(base_name=base_name)

    # Create bucket and enable versioning
    create_bucket_and_wait(s3_client, bucket_name)

    # Set bucket versioning to Enabled one time
    response = s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info(f"put_bucket_versioning response status: {response_status}")
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful put_bucket_versioning."

    # TODO: HACK: #notcool #eventual-consistency
    # make multiple ge_bucket_versioning requests to assure that the status is known to be Enabled
    versioning_status = probe_versioning_status(s3_client, bucket_name)
    assert versioning_status == "Enabled", f"Expected VersionConfiguration for bucket {bucket_name} to be Enabled, got {versioning_status}"

    # Upload a single object and get it's version
    object_key = "test-object.txt"
    content = b"Sample content for testing versioned object."
    object_version = put_object_and_wait(s3_client, bucket_name, object_key, content)
    if not object_version:
        logging.info(f"Bucket ${bucket_name} was not versioned before the object put, insisting with more objects...")
        object_version, object_key = replace_failed_put_without_version(s3_client, bucket_name, object_key, content)

    end_time = datetime.now()
    logging.warning(f"[versioned_bucket_with_one_object] Total setup time={end_time - start_time}")
    assert object_version, "Setup failed, could not get VersionId from put_object in versioned bucket"

    # Yield details to tests
    yield bucket_name, object_key, object_version

    # Cleanup
    try:
        # Delete all versions of the object
        logging.info(f"Deleting versions of {object_key}")
        delete_all_objects_with_version_and_wait(s3_client, bucket_name)
        # Delete the bucket
        logging.info(f"Deleting bucket {bucket_name}")
        delete_bucket_and_wait(s3_client, bucket_name)
    except Exception as e:
        logging.info(f"Cleanup error {e}")

@pytest.fixture
def versioned_bucket_with_one_object_cold_storage_class(s3_client, lock_mode):
    """
    Fixture to create a versioned bucket with one object on cold storage class for testing.
    
    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode for the bucket or objects (e.g., 'GOVERNANCE', 'COMPLIANCE')
    :return: Tuple containing bucket name, object key, and object version ID
    """
    start_time = datetime.now()
    base_name = "versioned-bucket-with-one-object"
    bucket_name = generate_unique_bucket_name(base_name=base_name)

    # Create bucket and enable versioning
    create_bucket_and_wait(s3_client, bucket_name)

    # Set bucket versioning to Enabled one time
    response = s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info(f"put_bucket_versioning response status: {response_status}")
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful put_bucket_versioning."

    # TODO: HACK: #notcool #eventual-consistency
    # make multiple ge_bucket_versioning requests to assure that the status is known to be Enabled
    versioning_status = probe_versioning_status(s3_client, bucket_name)
    assert versioning_status == "Enabled", f"Expected VersionConfiguration for bucket {bucket_name} to be Enabled, got {versioning_status}"

    # Upload a single object and get it's version
    object_key = "test-object.txt"
    content = b"v1"
    # Upload the object with GLACIER_IR storage class
    object_version = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass='GLACIER_IR'  # Specify the storage class as GLACIER_IR 
        )["VersionId"]
    if not object_version:
        logging.info(f"Bucket ${bucket_name} was not versioned before the object put, insisting with more objects...")
        object_version, object_key = replace_failed_put_without_version(s3_client, bucket_name, object_key, content)

    end_time = datetime.now()
    logging.warning(f"[versioned_bucket_with_one_object] Total setup time={end_time - start_time}")
    assert object_version, "Setup failed, could not get VersionId from put_object in versioned bucket"

    # Yield details to tests
    yield bucket_name, object_key, object_version

    # Cleanup
    try:
        # Delete all versions of the object
        logging.info(f"Deleting versions of {object_key}")
        delete_all_objects_with_version_and_wait(s3_client, bucket_name)
        # Delete the bucket
        logging.info(f"Deleting bucket {bucket_name}")
        delete_bucket_and_wait(s3_client, bucket_name)
    except Exception as e:
        logging.info(f"Cleanup error {e}")

@pytest.fixture
def bucket_with_one_object_and_lock_enabled(s3_client, lock_mode, versioned_bucket_with_one_object):
    bucket_name, object_key, object_version = versioned_bucket_with_one_object
    configuration = { 'ObjectLockEnabled': 'Enabled', }
    put_object_lock_configuration_with_determination(s3_client, bucket_name, configuration)
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
def bucket_with_lock(lockeable_bucket_name, s3_client, lock_mode, lock_wait_time):
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
    configuration = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {
            "DefaultRetention": {
                "Mode": lock_mode,
                "Days": retention_days
            }
        }
    }
    put_object_lock_configuration_with_determination(s3_client, bucket_name, configuration)

    # wait for the bucket lock change to be effective
    wait_time = lock_wait_time
    logging.info(f"Put bucket lock config might take time to propagate. Wait more {wait_time} seconds")
    time.sleep(wait_time)

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
    if not object_version:
        object_version, object_key = replace_failed_put_without_version(s3_client, bucket_name, object_key, object_content)

    assert object_version, "Setup failed, could not get VersionId from put_object in versioned bucket"

    # Verify that the object is uploaded and has a version ID
    if not object_version:
        pytest.fail("Uploaded object does not have a version ID")

    # Return bucket name, object key, and version ID
    return bucket_name, object_key, object_version
    
@pytest.fixture
def bucket_with_one_object_policy(multiple_s3_clients, policy_wait_time, request):
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
    #object_key = request.param.get("resource_key", "PolicyObject.txt")
    object_key = "object_key.txt"
    bucket_name = generate_unique_bucket_name(base_name=base_name)
    
    create_bucket_and_wait(client, bucket_name)
    put_object_and_wait(client, bucket_name, object_key, "42")    
    
    tenants = get_tenants(multiple_s3_clients)
    
    policy = change_policies_json(bucket=bucket_name, policy_args=request.param, tenants=tenants)
    client.put_bucket_policy(Bucket=bucket_name, Policy = policy)

    # TODO: HACK: #notcool #eventual-consistency wait for policy to be there
    registered_policy = get_policy_with_determination(client, bucket_name)
    logging.info(f"Registered policy after (get policy) consistency: {registered_policy}")
    wait_time = policy_wait_time
    logging.info(f"Receiving, 5 positive GETs is not a guarantee that the policy is in place. Wait more {wait_time} seconds")
    time.sleep(wait_time)

    # Yield the bucket name and object key to the test
    yield bucket_name, object_key
    
    # Teardown: delete the bucket after the test
    delete_policy_and_bucket_and_wait(client, bucket_name, policy_wait_time, request)




@pytest.fixture(params=[{ 'number_clients': 2 }])
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
    

## Fixtures for session scoped tests

@pytest.fixture(scope="session")
def session_test_params(request):
    """
    Loads test parameters from a config file or environment variable.
    """
    config_path = request.config.getoption("--config") or os.environ.get("CONFIG_PATH", "../params.example.yaml")
    
    profile = request.config.getoption("--profile") or os.environ.get("PROFILE", None)
    logging.info(f"Profile: {profile}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        if not profile:
            return config
        sufix = ["", "-second", "-sa"]
        for index, profile_index in enumerate(config["profiles"]):
            if "profile_name" in profile_index and index < 3:
                profile_index["profile_name"] = f"{profile}{sufix[index]}"
        
        return config

@pytest.fixture(scope="session")
def session_default_profile(session_test_params):
    """
    Returns the default profile from test parameters.
    """
    return session_test_params["profiles"][session_test_params.get("default_profile_index", 0)]

@pytest.fixture(scope="session")
def session_profile_name(session_default_profile):
    return (
        session_default_profile.get("profile_name")
        if session_default_profile.get("profile_name")
        else pytest.skip("This test requires a profile name")
    )

@pytest.fixture(scope="session")
def session_mgc_path(session_default_profile):
    """
    Validates and returns the path to the 'mgc' binary.
    """
    mgc_path_field_name = "mgc_path"
    if not session_default_profile.get(mgc_path_field_name):
        path = shutil.which("mgc")
    else:
        spec_dir = os.path.dirname(get_spec_path())
        path = os.path.join(spec_dir, session_default_profile.get(mgc_path_field_name))
    if not os.path.isfile(path):
        pytest.fail(f"The specified mgc_path '{path}' (absolute: {os.path.abspath(path)}) does not exist or is not a file.")
    return path

@pytest.fixture(scope="session")
def session_active_mgc_workspace(session_profile_name, session_mgc_path):
    # set the profile
    result = subprocess.run([session_mgc_path, "workspace", "set", session_profile_name],
                            capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("This test requires an mgc profile name")

    logging.info(f"mcg workspace set stdout: {result.stdout}")
    return profile_name

@pytest.fixture(scope="session")
def session_s3_client(session_default_profile):
    """
    Creates a boto3 S3 client using profile credentials or explicit config.
    """
    if "profile_name" in session_default_profile:
        session = boto3.Session(profile_name=session_default_profile["profile_name"])
    else:
        session = boto3.Session(
            region_name=session_default_profile["region_name"],
            aws_access_key_id=session_default_profile["aws_access_key_id"],
            aws_secret_access_key=session_default_profile["aws_secret_access_key"],
        )
    return session.client("s3", endpoint_url=session_default_profile.get("endpoint_url"))

@pytest.fixture(params=[{'object_key': 'test-object.txt'}], scope="session")
def session_bucket_with_one_object(request, session_s3_client):
    # this fixture accepts an optional request.param['object_key'] if you
    # need a custom specific key name for your test
    object_key = request.param['object_key']

    # Generate a unique bucket name and ensure it exists
    bucket_name = generate_unique_bucket_name(base_name="fixture-bucket")
    create_bucket_and_wait(session_s3_client, bucket_name)

    # Define the object content, then upload the object
    content = b"Sample content for testing presigned URLs."
    put_object_and_wait(session_s3_client, bucket_name, object_key, content)

    # Yield the bucket name and object details to the test
    yield bucket_name, object_key, content

    # Teardown: Delete the object and bucket after the test
    delete_object_and_wait(session_s3_client, bucket_name, object_key)
    delete_bucket_and_wait(session_s3_client, bucket_name)


@pytest.fixture(scope="session")
def session_versioned_bucket_with_one_object(session_s3_client):
    """
    Fixture to create a versioned bucket with one object for testing.
    
    :param s3_client: Boto3 S3 client
    :param lock_mode: Lock mode for the bucket or objects (e.g., 'GOVERNANCE', 'COMPLIANCE')
    :return: Tuple containing bucket name, object key, and object version ID
    """
    start_time = datetime.now()
    base_name = "versioned-bucket-with-one-object"
    bucket_name = generate_unique_bucket_name(base_name=base_name)

    # Create bucket and enable versioning
    create_bucket_and_wait(session_s3_client, bucket_name)

    # Set bucket versioning to Enabled one time
    response = session_s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info(f"put_bucket_versioning response status: {response_status}")
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful put_bucket_versioning."

    # TODO: HACK: #notcool #eventual-consistency
    # make multiple ge_bucket_versioning requests to assure that the status is known to be Enabled
    versioning_status = probe_versioning_status(session_s3_client, bucket_name)
    assert versioning_status == "Enabled", f"Expected VersionConfiguration for bucket {bucket_name} to be Enabled, got {versioning_status}"

    # Upload a single object and get it's version
    object_key = "test-object.txt"
    content = b"Sample content for testing versioned object."
    object_version = put_object_and_wait(session_s3_client, bucket_name, object_key, content)
    if not object_version:
        logging.info(f"Bucket ${bucket_name} was not versioned before the object put, insisting with more objects...")
        object_version, object_key = replace_failed_put_without_version(session_s3_client, bucket_name, object_key, content)

    end_time = datetime.now()
    logging.warning(f"[versioned_bucket_with_one_object] Total setup time={end_time - start_time}")
    assert object_version, "Setup failed, could not get VersionId from put_object in versioned bucket"

    # Yield details to tests
    yield bucket_name, object_key, object_version

    # Cleanup
    try:
        # Delete all versions of the object
        logging.info(f"Deleting versions of {object_key}")
        delete_all_objects_with_version_and_wait(s3_client, bucket_name)
        # Delete the bucket
        logging.info(f"Deleting bucket {bucket_name}")
        delete_bucket_and_wait(s3_client, bucket_name)
    except Exception as e:
        logging.info(f"Cleanup error {e}")

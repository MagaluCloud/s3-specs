import pytest
import boto3
import logging
from s3_specs.docs.s3_helpers import generate_unique_bucket_name, get_tenants
from s3_specs.docs.tools.permission import generate_policy
from botocore.exceptions import ClientError
from botocore.config import Config

@pytest.fixture
def get_bulk_s3_clients(session_test_params):
    clients = [p for p in session_test_params["profiles"][:2]]

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
        
        client = session.client("s3", endpoint_url=client.get("endpoint_url"))
        
        def custom_request(request, **kwargs):
            request.headers['X-Force-Container-Delete'] = str(True).lower()

        event_system = client.meta.events
        event_system.register_first('request-created', custom_request)

        sessions.append(client)
        
    return sessions

def create_bucket(owner, lock_enabled=False):
    try:
        bucket_name = generate_unique_bucket_name(base_name="test-bulk-delete")

        create_params = {
            "Bucket": bucket_name
        }

        if lock_enabled:
            create_params["ObjectLockEnabledForBucket"] = True

        response = owner.create_bucket(**create_params)

        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
        assert status_code == 200, f"{status_code} should be 200"

        if lock_enabled:
            # Verificação opcional
            lock_conf = owner.get_object_lock_configuration(Bucket=bucket_name)
            assert "ObjectLockConfiguration" in lock_conf

        return bucket_name
    except ClientError as e:
        logging.info(f"ClientError during bucket creation -> {e}")
        pytest.fail("ClientError during bucket creation")
    except Exception as e:
        logging.info(f"Error during bucket creation -> {e}")
        pytest.fail("Error during bucket creation")

@pytest.fixture
def create_bucket_with_all_permissions_for_second_account(request, multiple_s3_clients):
    owner, second = multiple_s3_clients

    permission = request.param

    owner_tenant, second_tenant = get_tenants([owner, second])

    logging.info(f"Owner Tenant: {owner_tenant}")
    logging.info(f"Second Tenant: {second_tenant}")

    bucket_name = create_bucket(owner)
    
    if permission in ["both", "acl"]:
        response = owner.put_bucket_acl(Bucket=bucket_name, GrantFullControl=f'id={second_tenant}')
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
        assert status_code == 200, f"{status_code} should be 200"
        
    if permission in ["both", "policy"]:
        bucket_policy = generate_policy(effect='Allow', principals=second_tenant, actions='s3:*', resources=f'{bucket_name}/*')

        response = owner.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
        assert status_code == 204, f"{status_code} should be 204"


    logging.info(f"Waiting if second user can see the bucket {bucket_name}")
    waiter = second.get_waiter('bucket_exists')
    waiter.wait(
        Bucket=bucket_name,
        WaiterConfig={
            'Delay': 60
        }
    )

    response = second.put_object(Bucket=bucket_name, Key='test.txt', Body='teste put object')
    status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    logging.info(f"Put object using second user got {status_code} as status code")
    assert status_code == 200, f"{status_code} should be 200"

    return bucket_name
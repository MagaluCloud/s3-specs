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

def assertStatusCode(response, target_status_code):
    status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    assert status_code == target_status_code, f"{status_code} should be {target_status_code}"
        

@pytest.fixture
def setup_bucket(request, multiple_s3_clients):
    owner, second = multiple_s3_clients
    owner_tenant, second_tenant = get_tenants([owner, second])
    
    test_args = request.node.callspec.params

    permission = test_args.get("permission")
    state = test_args.get("state")
    access = test_args.get("access")
    lock = test_args.get("lock")

    bucket_name = create_bucket(owner, lock)

    if state == "cold":
        response = owner.put_object(
            Bucket=bucket_name,
            Key="teste.txt",
            Body="teste", 
            StorageClass="GLACIER_IR"
        )
        assertStatusCode(response, 200)
    
    elif state == "multipart":
        response = owner.create_multipart_upload(
            Bucket=bucket_name,
            Key="test.txt"
        )

        assertStatusCode(response, 200)

        part1 = b"A"*1024*1024*7
        part2 = b"A"*1024*1024*5
        parts = []

        response_upload_part = owner.upload_part(
            Bucket=bucket_name,
            Key="test.txt",
            Body=part1,
            PartNumber=1,
            UploadId = response['UploadId'],
        )
        parts.append({'ETag': response_upload_part['ETag'], 'PartNumber': 1})

        assertStatusCode(response_upload_part, 200)

        response_upload_part = owner.upload_part(
            Bucket=bucket_name,
            Key="test.txt",
            Body=part2,
            PartNumber=2,
            UploadId = response['UploadId'],
        )
        parts.append({'ETag': response_upload_part['ETag'], 'PartNumber': 2})

        assertStatusCode(response_upload_part, 200)

        response_complete_multipart = owner.complete_multipart_upload(
            Bucket=bucket_name,
            UploadId = response['UploadId'],
            MultipartUpload={'Parts': parts},
            Key="teste.txt"
        )

        assertStatusCode(response_complete_multipart, 200)

    if access != "owner":
        if permission == "acl":
            response = owner.put_bucket_acl(Bucket=bucket_name, GrantFullControl=f'id={second_tenant}')
            assertStatusCode(response, 200)
        else:
            bucket_policy = generate_policy(effect='Allow', principals=second_tenant, actions='s3:*', resources=bucket_name)
            response = owner.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
            assertStatusCode(response, 204)
    

    yield bucket_name

    try:
        owner.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        logging.info(f"Error during delete: {e}")
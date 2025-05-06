import botocore
import pytest
from botocore.exceptions import ClientError
import logging
from s3_specs.docs.tools.bulk_delete import get_bulk_s3_clients, create_bucket, create_bucket_with_all_permissions_for_second_account

def test_delete_bucket_as_owner(get_bulk_s3_clients):
    owner, _ = get_bulk_s3_clients

    bucket_name = create_bucket(owner)

    response = owner.delete_bucket(Bucket=bucket_name)
    
    status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    assert status_code == 204, f"{status_code} should be 204"


def test_delete_bucket_as_owner_with_lock(get_bulk_s3_clients):
    owner, _ = get_bulk_s3_clients

    bucket_name = create_bucket(owner=owner, lock_enabled=True)

    with pytest.raises((botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError)) as exc_info:
        response = owner.delete_bucket(Bucket=bucket_name)
        logging.info(response)
    
    logging.info(exc_info)
    assert any(msg in str(exc_info.value) for msg in ["InvalidBucketState", "Invalid bucket state"])


@pytest.mark.parametrize(
    "create_bucket_with_all_permissions_for_second_account",
    [
        pytest.param("acl", id="permission_acl_only"),
        pytest.param("policy", id="permission_policy_only"),
        pytest.param("both", id="permission_acl_and_policy"),
    ],
    indirect=True
)
def test_delete_bucket_not_being_bucket_owner(get_bulk_s3_clients, create_bucket_with_all_permissions_for_second_account):
    owner, second = get_bulk_s3_clients
        
    bucket_name = create_bucket_with_all_permissions_for_second_account
    
    try:
        with pytest.raises((botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError)) as exc_info:
            try:
                response = second.delete_bucket(Bucket=bucket_name)
                logging.info(response)
                pytest.fail("Expected ClientError or ParamValidationError when deleting a bucket without ownership, but no exception was raised.")
            except Exception as e:
                raise e

        logging.info(exc_info)
        assert any(msg in str(exc_info.value) for msg in ["AccessDenied","AccessDeniedByBucketPolicy", "Access denied by bucket policy"])
    finally:
        try:
            logging.info("deleting bucket")
            response = owner.delete_bucket(Bucket=bucket_name)
            logging.info(f"response: {response}")
        except Exception as e:
            logging.info(f"error during deletion: {e}")
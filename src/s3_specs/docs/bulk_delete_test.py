import botocore
import pytest
from botocore.exceptions import ClientError
import logging
from s3_specs.docs.tools.bulk_delete import setup_bucket, get_bulk_s3_clients, create_bucket
import itertools

pytestmark = [pytest.mark.bulk_delete, pytest.mark.quick, pytest.mark.skip_if_dev, pytest.mark.homologacao]

permissions = [pytest.param("acl", id="with-acl"), pytest.param("policy", id="with-policy")]
states = [
    pytest.param("empty", id="bucket-empty"),
    pytest.param("multipart", id="bucket-with-multipart"),
    pytest.param("cold", marks=pytest.mark.only_run_in_region("br-se1", "us-east-1"), id="bucket-with-obj-cold")
]
accesses = [pytest.param("owner", id="deleting-as-owner"), pytest.param("second", id="deleting-as-secondary-account")]
locks = [pytest.param(True, id="locking-enabled"), pytest.param(False, id="without-locking")]

param_values = []
param_ids = []

for perm, state, acc, lock in itertools.product(permissions, states, accesses, locks):
    combined_id = f"{perm.id}-and-{state.id}-and-{acc.id}-and-{lock.id}"

    # Coletar todos os marks (se tiver) e aplicar ao param final
    all_marks = []
    for p in (perm, state, acc, lock):
        if hasattr(p, "marks"):
            all_marks.extend(p.marks)

    param = pytest.param(
        perm.values[0], state.values[0], acc.values[0], lock.values[0],
        id=combined_id,
        marks=all_marks  # Reaplica todos os marks ao param final
    )

    param_values.append(param)
    param_ids.append(combined_id)

@pytest.mark.parametrize(
    ("permission", "state", "access", "lock"),
    param_values,
    ids=param_ids,
)
def test_delete_bucket(setup_bucket, get_bulk_s3_clients, permission, state, access, lock):
    bucket_name = setup_bucket

    owner, second = get_bulk_s3_clients
    users = {"owner":owner, "second":second}


    if lock or access != "owner":
        with pytest.raises((botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError)) as exc_info:
            try:
                response = users[access].delete_bucket(Bucket=bucket_name)
                logging.info(response)
                pytest.fail("Expected ClientError or ParamValidationError when deleting a bucket without ownership, but no exception was raised.")
            except Exception as e:
                raise e
            
        logging.info(exc_info)
        assert any(msg in str(exc_info.value) for msg in ['AccessDeniedByBucketPolicy', 'AccessDenied'])
    

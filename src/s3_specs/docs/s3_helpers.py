import os

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import uuid
import logging
import pytest
import yaml
from pathlib import Path
import ipynbname
import json
import time
from s3_specs.docs.tools.utils import generate_valid_bucket_name

def get_spec_path():
    spec_path = os.getenv("SPEC_PATH")
    if spec_path:
        return Path(spec_path).resolve()
    # Fallback for live Jupyter Lab Notebook execution
    try:
        return str(ipynbname.path())
    except Exception:
        # fallback to local path
        return "./"

def run_example(dunder_name, test_name, config="../params.example.yaml"):
    if dunder_name == "__main__":
        # When executing a notebook pass config path as env var instead of pytest custom arg
        os.environ["CONFIG_PATH"] = os.environ.get("CONFIG_PATH", config)

        # Run pytest without the --config argument
        pytest.main([
            "-qq", 
            "--color", "no", 
            "--pytest-durations", "0",
            # "-s", 
            # "--log-cli-level", "INFO",
            f"{get_spec_path()}::{test_name}"
        ])
 
def generate_unique_bucket_name(base_name="my-unique-bucket"):
    base_name = generate_valid_bucket_name(base_name)

    return base_name.lower()



def delete_bucket_and_wait(s3_client, bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        logging.info("Bucket already deleted by someone else.")
        return
    except Exception as e:
        logging.info(f"delete bucket errored with: {e}")

    waiter = s3_client.get_waiter('bucket_not_exists')
    try:
        waiter.wait(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")
    except Exception as e:
        logging.info(f"delete bucket waiter errored with: {e}")

def create_bucket(s3_client, bucket_name):
    # anything different than us-east-1 must have LocationConstraint on aws
    region = s3_client.meta.region_name
    if (region != "us-east-1"):
        return s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
    return s3_client.create_bucket(Bucket=bucket_name)

def create_bucket_and_wait(s3_client, bucket_name):
    try:
        create_bucket(s3_client, bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        logging.info(f"Bucket '{bucket_name}' already exists and is owned by you.")
    except s3_client.exceptions.BucketAlreadyExists:
        raise Exception(f"Bucket '{bucket_name}' already exists and is owned by someone else.")
    except Exception as e:
        logging.info(f"create bucket errored with: {e}")

    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' confirmed as created.")

def delete_object_and_wait(s3_client, bucket_name, object_key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    except s3_client.exceptions.NoSuchKey:
        logging.info(f"Object '{object_key}' already deleted or not found.")
        return
    except Exception as e:
        logging.info(f"delete object got error: {e}")

    waiter = s3_client.get_waiter('object_not_exists')
    try:
        waiter.wait(Bucket=bucket_name, Key=object_key)
    except Exception as e:
        logging.info(f"delete waiter got error: {e}")
    logging.info(f"Object '{object_key}' in bucket '{bucket_name}' confirmed as deleted.")

def delete_all_objects_and_wait(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            delete_object_and_wait(s3_client, bucket_name, obj['Key'])
 
def delete_policy_and_bucket_and_wait(s3_client, bucket_name, policy_wait_time, request):
    retries = 3
    sleeptime = 5
    for attempt_number in range(retries):   
        try:
            change_policies_json(bucket_name, {"policy_dict": request.param['policy_dict'], "actions": ["s3:GetObjects", "*"], "effect": "Allow"}, tenants=["*"])
            logging.info(f"deleting policy of bucket {bucket_name}...")
            s3_client.delete_bucket_policy(Bucket=bucket_name)
            break
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                logging.info(f"No policy found for bucket '{bucket_name}'.")
                break
            else:
                time.sleep(sleeptime * attempt_number)
                continue 
        except Exception as e:
            logging.info(f"delete policy errored with: {e}")
           
    logging.info(f"waiting for policy delete to propagate, {policy_wait_time} seconds")
    time.sleep(policy_wait_time)
    logging.info(f"deleting all objects...")
    delete_all_objects_and_wait(s3_client, bucket_name)
    logging.info(f"deleting bucket...")
    delete_bucket_and_wait(s3_client, bucket_name)

def put_object_and_wait(s3_client, bucket_name, object_key, content):
    """
    Upload an object to S3 and wait for it to be available.

    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the bucket
    :param object_key: Key (name) of the object
    :param content: Content of the object (bytes)
    :param storage_class: Storage Class of the object, STANDARD by default 
    :return: Version ID of the object if versioning is enabled, otherwise None
    """
    # Upload the object
    put_response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
    logging.info(f"put_object response: {put_response}")
    version_id = put_response.get("VersionId", None)

    # Wait for the object to exist
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=object_key)

    # Log confirmation
    logging.info(
        f"Object '{object_key}' in bucket '{bucket_name}' confirmed as uploaded. Version ID: {version_id}"
    )

    return version_id

def cleanup_old_buckets(s3_client, base_name, lock_mode=None, retention_days=1):
    """
    Delete buckets with the specified base name that are older than the retention period.
    Attempt to delete versions and delete markers, retry with governance bypass if needed.

    :param s3_client: Boto3 S3 client
    :param base_name: Prefix of the bucket names to target
    :param lock_mode: Lock mode ('GOVERNANCE', 'COMPLIANCE', or None)
    :param retention_days: Age threshold for buckets to be cleaned up (ignored for GOVERNANCE)
    """

    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        if bucket_name.startswith(f"test-{base_name}"):
            creation_date = bucket['CreationDate']
            age_threshold = datetime.now(creation_date.tzinfo) - timedelta(days=retention_days)

            if lock_mode == "GOVERNANCE" or creation_date < age_threshold:
                try:
                    # Get bucket versioning info
                    bucket_versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)

                    # If bucket is versioned, delete all object versions and delete markers
                    if bucket_versioning.get('Status') == 'Enabled':
                        paginator = s3_client.get_paginator('list_object_versions')
                        for page in paginator.paginate(Bucket=bucket_name):
                            # Delete object versions
                            for version in page.get('Versions', []):
                                delete_version(
                                    s3_client, bucket_name, version, lock_mode
                                )
                            # Delete markers
                            for marker in page.get('DeleteMarkers', []):
                                delete_version(
                                    s3_client, bucket_name, marker, lock_mode
                                )

                    # Delete the bucket itself
                    s3_client.delete_bucket(Bucket=bucket_name)
                    logging.info(f"Deleted old bucket '{bucket_name}' created on {creation_date}")
                except ClientError as e:
                    logging.warning(f"Could not delete bucket '{bucket_name}': {e}")
                except Exception as e:
                    logging.info(f"get bucket versioning errored with: {e}")

def delete_version(s3_client, bucket_name, version, lock_mode):
    """
    Attempt to delete an object version or delete marker.
for _ in range(5):

    :param s3_client: Boto3 S3 client.
    :param bucket_name: Name of the bucket.
    :param version: The version or delete marker to delete.
    :param lock_mode: Lock mode ('GOVERNANCE', 'COMPLIANCE', or None).
    """
    version_id = version['VersionId']
    try:
        # Attempt to delete the version
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=version['Key'],
            VersionId=version_id
        )
        logging.info(f"Deleted version {version_id} of object {version['Key']} in bucket {bucket_name}")
    except ClientError as e:
        # Retry deletion with governance bypass if necessary
        if e.response["Error"]["Code"] == "AccessDenied" and lock_mode == "GOVERNANCE":
            logging.info(f"Retrying deletion of version {version_id} with governance bypass")
            s3_client.delete_object(
                Bucket=bucket_name,
                Key=version['Key'],
                VersionId=version_id,
                BypassGovernanceRetention=True
            )
        else:
            logging.warning(
                f"Failed to delete version {version_id} of object {version['Key']} in bucket {bucket_name}: {e}"
            )
    except Exception as e:
        logging.info(f"delete object errored with: {e}")

def change_policies_json(bucket, policy_args: dict, tenants: list) -> json:
    
    """
    From a policy changes its contest with the requested params and transform it into a JSON.

    :param policy_args: list of args to put into the policy json.
    :param tenants: list of tenants of principals.
    :return json: json containing a policy
    """
    
    #parse the request
    policy = policy_args['policy_dict']
    effect = policy_args['effect']
    actions = policy_args['actions']
    
    #change arguments inside of the policy dict
    policy["Statement"][0]["Effect"] = effect
    policy["Statement"][0]["Principal"] = tenants
    policy["Statement"][0]["Action"] = actions
    policy["Statement"][0]["Resource"] = [bucket + "/*", bucket]
        
    logging.info(f"POLICY: {json.dumps(policy)}")
    return json.dumps(policy)


def get_tenants(multiple_s3_clients):
    """
    Get the tenant from the test_params and return a list of all client's tenants.

    :param test_params: The test parameters.
    :param client_number: The client number.
    :return: The tenant for the client number.
    """
    bucket_list = []

    for i, client in enumerate(multiple_s3_clients):        
        id = client.list_buckets()
        bucket_list.append(id['Owner']['ID'])
        
    return bucket_list
    

def update_existing_keys(main_dict, sub_dict):
    """
    Update the values in `main_dict` with the values from `sub_dict` 
    only for keys that already exist in `main_dict`.

    :param main_dict: The dictionary to be updated.
    :param sub_dict: The dictionary providing new values for existing keys in `main_dict`.
    :return: The updated `main_dict`.
    """
    for key in main_dict.keys():
        if key in sub_dict:
            main_dict[key] = sub_dict[key]

    return main_dict

# TODO: not cool, #eventualconsistency
# Sometimes a test of feature needs to put an object on a versioned bucket, and is expected that
# a put object on a versioned bucket returns a version id in the response of the PUT request.
#
# However, due to eventual consistency, even after bucket creation and a positive response on the 
# versioning status of such bucket returns the Enabled status. Not all replicas know that the bucket
# is a versioned enabled one, and so, a put object request will return without version ID.
#
# This helper function is a workaround that will attempt multiple times to put an object in a bucket
# and if the response does not include a version ID, will try to put another object, with another
# key on the same bucket, waiting some seconds between re-attempts. Until the put object response
# returns a version ID or until the number of maximum retries is reached.
def replace_failed_put_without_version(s3_client, bucket_name, object_key, object_content):

    retries = 0
    interval_multiplier = 3 # seconds
    start_time = datetime.now()
    object_version = None
    while not object_version and retries < 10:
        retries += 1

        # create a new object key
        new_object_key = f"test_object_{retries}.txt"

        logging.info(f"attempt ({retries}): key:{new_object_key}")
        wait_time = retries * retries * interval_multiplier
        logging.info(f"wait {wait_time} seconds")
        time.sleep(wait_time)

        try:
            # delete object (marker?) on the strange object without version id
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        except Exception as e:
            logging.info(f"previous put object without version could not be deleted: {e}")

        # put the object again in the hopes that this time it will have a version id
        response = s3_client.put_object(Bucket=bucket_name, Key=new_object_key, Body=object_content)
        logging.info(f"put_object response: {response}")

        # check if it has version id
        object_version = response.get("VersionId")
        if not object_version:
            # try to get the object version not returned by the put_object with a head_object
            logging.info(f"put response dont have version, wait {wait_time} seconds before head_object")
            time.sleep(wait_time)
            logging.info(f"head_object Key={new_object_key}...")
            head_object_response = s3_client.head_object(Bucket=bucket_name, Key=new_object_key)
            logging.info(f"Head object {new_object_key}: {head_object_response}")
            object_version = head_object_response.get("VersionId")

        logging.info(f"Object {new_object_key} in bucket {bucket_name} confirmed as uploaded. Version ID: {object_version}")
    end_time = datetime.now()
    logging.warning(f"[replace_failed_put_without_version] Total consistency wait time={end_time - start_time}")

    return object_version, new_object_key

# TODO: review when #eventualconsistency stops being so bad
def put_object_lock_configuration_with_determination(s3_client, bucket_name, configuration):
    retries = 0
    interval_multiplier = 3 # seconds
    response = None
    start_time = datetime.now()
    while retries < 10:
        retries += 1
        try:
            response = s3_client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration=configuration
            )
            break
        except Exception as e:
            logging.error(f"Error ({retries}): {e}")
            wait_time = retries * retries * interval_multiplier
            logging.info(f"wait {wait_time} seconds")
            time.sleep(wait_time)
    end_time = datetime.now()
    logging.warning(f"[put_object_lock_configuration_with_determination] Total consistency wait time={end_time - start_time}")
    return response

# TODO: review when #eventualconsistency stops being so bad
def get_policy_with_determination(s3_client, bucket_name):
    retries = 0
    interval_multiplier = 3 # seconds
    response = None
    start_time = datetime.now()
    while retries < 20:
        retries += 1
        try:
            # make 5 GETs in an attempt to get responses from all replicas
            response1 = s3_client.get_bucket_policy(Bucket=bucket_name)
            logging.info(response1)
            response2 = s3_client.get_bucket_policy(Bucket=bucket_name)
            logging.info(response2)
            response3 = s3_client.get_bucket_policy(Bucket=bucket_name)
            logging.info(response3)
            response4 = s3_client.get_bucket_policy(Bucket=bucket_name)
            logging.info(response4)
            response5 = s3_client.get_bucket_policy(Bucket=bucket_name)
            logging.info(response5)
            break
        except Exception as e:
            logging.error(f"[get_policy_with_determination] Error ({retries}): {e}")
            wait_time = retries * retries * interval_multiplier
            logging.info(f"wait {wait_time} seconds")
            time.sleep(wait_time)
    end_time = datetime.now()
    logging.warning(f"[get_policy_with_determination] Total consistency wait time={end_time - start_time}")
    assert response1 and response1.get("Policy"), "Setup error, bucket dont have policy"
    return response1

# TODO: review when #eventualconsistency stops being so bad
def get_object_retention_with_determination(s3_client, bucket_name, object_key):
    retries = 0
    interval_multiplier = 3 # seconds
    response = None
    start_time = datetime.now()
    while retries < 20:
        retries += 1
        try:
            # make 5 GETs in an attempt to get responses from all replicas
            response = s3_client.get_object_retention( Bucket=bucket_name, Key=object_key,)
            response2 = s3_client.get_object_retention( Bucket=bucket_name, Key=object_key,)
            response3 = s3_client.get_object_retention( Bucket=bucket_name, Key=object_key,)
            response4 = s3_client.get_object_retention( Bucket=bucket_name, Key=object_key,)
            response5 = s3_client.get_object_retention( Bucket=bucket_name, Key=object_key,)
            break
        except Exception as e:
            logging.error(f"[get_object_retention_with_determination] Error ({retries}): {e}")
            wait_time = retries * retries * interval_multiplier
            logging.info(f"wait {wait_time} seconds")
            time.sleep(wait_time)
    end_time = datetime.now()
    logging.warning(f"[get_object_retention_with_determination] Total consistency wait time={end_time - start_time}")
    assert response and response.get("Retention"), "Setup error, object dont have retention"
    return response


# TODO: review when #eventualconsistency stops being so bad
def get_object_lock_configuration_with_determination(s3_client, bucket_name):
    retries = 0
    interval_multiplier = 3 # seconds
    response = None
    start_time = datetime.now()
    while retries < 20:
        retries += 1
        try:
            # make 5 GETs in an attempt to get responses from all replicas
            response = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            response2 = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            response3 = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            response4 = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            response5 = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            break
        except Exception as e:
            logging.error(f"[get_object_lock_configuration_with_determination] Error ({retries}): {e}")
            wait_time = retries * retries * interval_multiplier
            logging.info(f"wait {wait_time} seconds")
            time.sleep(wait_time)
    end_time = datetime.now()
    logging.warning(f"[get_object_lock_configuration_with_determination] Total consistency wait time={end_time - start_time}")
    return response

def probe_versioning_status(s3_client, bucket_name):
    start_time = datetime.now()
    retries = 0
    interval_multiplier = 1 # seconds
    multiple_enabled_statuses = False # stopping condition, multiple requests must return the same value (Enabled)
    while not multiple_enabled_statuses and retries < 20:
        retries += 1
        wait_time = retries * retries * interval_multiplier
        logging.info(f"Attempt {retries}, wait {wait_time} seconds")
        time.sleep(wait_time)
        multiple_enabled_statuses = True
        for request_count in range(10):
            logging.info(f"get_bucket_versioning request: {request_count}")
            response = s3_client.get_bucket_versioning(Bucket=bucket_name)
            response_status = response["ResponseMetadata"]["HTTPStatusCode"]
            logging.info(f"get_bucket_versioning response status: {response_status}")
            assert response_status == 200, "Expected HTTPStatusCode 200 for successful put_bucket_versioning."
            logging.info(f"get_bucket_versioning response: {response}")
            response_versioning_status = response.get("Status", None)
            logging.info(f"get_bucket_versioning versioning status for bucket {bucket_name} is {response_versioning_status}")
            if not response_versioning_status:
                logging.info(f"consistency not yet reached")
                multiple_enabled_statuses = False
                break

    end_time = datetime.now()
    logging.warning(f"[wait_for_versioning_status] Total wait time={end_time - start_time}")
    return response_versioning_status

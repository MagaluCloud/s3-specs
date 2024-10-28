import boto3
from datetime import datetime, timedelta
import uuid
import logging
import pytest
import yaml

def run_example(dunder_name, spec_name, example_name, config="params.yaml"):
    if dunder_name == "__main__":
        with open(config, "r") as f:
            params = yaml.safe_load(f)
        profile_name = params.get("profile_name", "default")
        docs_dir = params.get("docs_dir", ".")
        pytest.main(["-qq", "--color", "no", "-s", "--log-cli-level", "INFO", "--config", f"{config}", f"{docs_dir}/{spec_name}_test.py::{example_name}"])
        # pytest.main(["-qq", "--color", "no", "-s", "--profile", f"{profile_name}", f"{docs_dir}/{spec_name}_test.py::{example_name}"])

def print_timestamp():
    logging.info(f'execution started at {datetime.datetime.now()}')

def generate_unique_bucket_name(base_name="my-unique-bucket"):
    unique_id = uuid.uuid4().hex[:6]  # Short unique suffix
    return f"{base_name}-{unique_id}"

def delete_bucket_and_wait(s3_client, bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        logging.info("Bucket already deleted by someone else.")
        return

    waiter = s3_client.get_waiter('bucket_not_exists')
    waiter.wait(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")

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

    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' confirmed as created.")

def delete_object_and_wait(s3_client, bucket_name, object_key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    except s3_client.exceptions.NoSuchKey:
        logging.info(f"Object '{object_key}' already deleted or not found.")
        return

    waiter = s3_client.get_waiter('object_not_exists')
    waiter.wait(Bucket=bucket_name, Key=object_key)
    logging.info(f"Object '{object_key}' in bucket '{bucket_name}' confirmed as deleted.")

def put_object_and_wait(s3_client, bucket_name, object_key, content):
    # Put the object in the bucket
    put_response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
    version_id = put_response.get("VersionId")

    # Wait for the object to exist
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=object_key)
    logging.info(f"Object '{object_key}' in bucket '{bucket_name}' confirmed as uploaded.")
    return version_id


def cleanup_old_buckets(s3_client, base_name, retention_days=1):
    """
    Delete buckets with the specified base name that are older than the retention period.
    Only deletes buckets if they are not locked.
    """
    # List and check buckets with the specified prefix
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'].startswith(base_name):
            creation_date = bucket['CreationDate']
            if creation_date < datetime.now(creation_date.tzinfo) - timedelta(days=retention_days):
                try:
                    # Attempt to delete older bucket if lock has expired
                    delete_all_objects(s3_client, bucket['Name'])
                    s3_client.delete_bucket(Bucket=bucket['Name'])
                    logging.info(f"Deleted old bucket '{bucket['Name']}' created on {creation_date}")
                except s3_client.exceptions.ClientError as e:
                    logging.info(f"Could not delete bucket '{bucket['Name']}': {e}")


def teardown_versioned_bucket_with_lock_config(s3_client, bucket_name, lock_mode):
    # Teardown logic for bucket cleanup
    try:
        # Check lock configuration
        lock_config = s3_client.get_object_lock_configuration(Bucket=bucket_name)
        lock_enabled = lock_config.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled") == "Enabled"
        
        # Proceed with deletion based on lock mode
        if lock_enabled and lock_mode == "GOVERNANCE":
            logging.info(f"Deleting objects in '{bucket_name}' with BypassGovernanceRetention.")
            versions = s3_client.list_object_versions(Bucket=bucket_name)
            for version in versions.get("Versions", []):
                s3_client.delete_object(
                    Bucket=bucket_name, 
                    Key=version["Key"], 
                    VersionId=version["VersionId"], 
                    BypassGovernanceRetention=True
                )
            for delete_marker in versions.get("DeleteMarkers", []):
                s3_client.delete_object(
                    Bucket=bucket_name, 
                    Key=delete_marker["Key"], 
                    VersionId=delete_marker["VersionId"], 
                    BypassGovernanceRetention=True
                )
        elif lock_enabled:
            logging.info(f"Bucket '{bucket_name}' is in COMPLIANCE mode; skipping deletion.")

        # Delete bucket if possible
        logging.info(f"Deleting bucket: {bucket_name}")
        s3_client.delete_bucket(Bucket=bucket_name)

    except botocore.exceptions.ClientError as e:
        logging.warning(f"Failed to delete bucket '{bucket_name}': {e}")

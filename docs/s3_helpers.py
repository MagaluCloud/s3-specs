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

def get_spec_path():
    spec_path = os.getenv("SPEC_PATH")
    if spec_path:
        return Path(spec_path).resolve()
    # Fallback for live Jupyter Lab Notebook execution
    try:
        return str(ipynbname.path())
    except Exception:
        # fallback to local path
        return "."

def run_example(dunder_name, test_name, config="../params.example.yaml"):
    if dunder_name == "__main__":
        # When executing a notebook pass config path as env var instead of pytest custom arg
        os.environ["CONFIG_PATH"] = config

        # Run pytest without the --config argument
        pytest.main([
            "-qq", 
            "--color", "no", 
            # "-s", 
            # "--log-cli-level", "INFO",
            f"{get_spec_path()}::{test_name}"
        ])
 

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
    """
    Upload an object to S3 and wait for it to be available.

    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the bucket
    :param object_key: Key (name) of the object
    :param content: Content of the object (bytes)
    :return: Version ID of the object if versioning is enabled, otherwise None
    """
    # Upload the object
    put_response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
    version_id = put_response.get("VersionId", None)

    # Wait for the object to exist
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=object_key)

    # Log confirmation
    logging.info(
        f"Object '{object_key}' in bucket '{bucket_name}' confirmed as uploaded. "
        f"Version ID: {version_id}"
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
        if bucket_name.startswith(base_name):
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

def delete_version(s3_client, bucket_name, version, lock_mode):
    """
    Attempt to delete an object version or delete marker.

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


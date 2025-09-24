import os
import subprocess
import boto3
import concurrent.futures
import datetime
import sys
from botocore.exceptions import BotoCoreError, ClientError
from botocore.config import Config

def list_old_test_buckets(profile_name, connect_timeout_sec=60, read_timeout_sec=600):
    """List 'test-' prefixed buckets older than 6 hours."""
    # Crie um objeto de configuração com os timeouts desejados
    s3_config = Config(
        connect_timeout=connect_timeout_sec,
        read_timeout=read_timeout_sec,
        retries = {
            'max_attempts': 5,  # Exemplo: tentar até 5 vezes
            'mode': 'standard'
        }
    )
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3', config=s3_config)
    try:
        prefixes = [
            "existing-bucket",
            "fixture-bucket",
            "lockeable-bucket",
            "policy-bucket",
            "versioned-bucket",
            "test"
        ]

        response = s3.list_buckets(
            MaxBuckets=10000
        )
        six_hours_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=6)

        return [
            bucket['Name']
            for bucket in response['Buckets']
            if bucket['Name'].startswith(tuple(prefixes)) and bucket['CreationDate'] < six_hours_ago
        ]
    except (BotoCoreError, ClientError) as e:
        print(f"Error listing buckets: {e}")
        return []

def delete_bucket_policy(s3, bucket_name):
    """Delete bucket policy if it exists."""
    try:
        print(f"Deleting bucket policy for `{bucket_name}`")
        s3.delete_bucket_policy(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucketPolicy':
            print(f"⚠️ Failed to delete policy for {bucket_name}: {e}")

def delete_all_objects(s3, bucket_name):
    """Delete all objects first, then delete versions if needed."""
    paginator = s3.get_paginator('list_objects_v2')
    print(f"Deleting all object in bucket `{bucket_name}`")

    try:
        for page in paginator.paginate(Bucket=bucket_name):
            objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])] 
            if objects:
                response = s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
                if "Errors" in response:
                    for error in response["Errors"]:
                        print(f"⚠️ Failed to delete object {error['Key']} in {bucket_name}: {error['Message']}")
    except ClientError as e:
        print(f"⚠️ Failed to delete objects in {bucket_name}: {e}")

    return delete_all_object_versions(s3, bucket_name)

def delete_all_object_versions(s3, bucket_name):
    """Delete all object versions and delete markers, suppressing unnecessary output."""
    paginator = s3.get_paginator('list_object_versions')
    locked_objects = []
    print(f"Deleting all object versions of `{bucket_name}`")

    try:
        for page in paginator.paginate(Bucket=bucket_name):
            objects = [
                {'Key': obj['Key'], 'VersionId': obj['VersionId']}
                for obj in page.get('Versions', []) + page.get('DeleteMarkers', [])
            ]
            if objects:
                print(f"Deleting {objects=}")
                response = s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
                if "Errors" in response:
                    for error in response["Errors"]:
                        if error["Code"] == "AccessDenied":
                            locked_objects.append(error['Key'])
                        print(f"⚠️ Failed to delete object {error['Key']} in {bucket_name}: {error['Message']}")
    except ClientError as e:
        print(f"⚠️ Failed to retrieve object versions in {bucket_name}: {e}")

    print(f"Done deleting versions of bucket `{bucket_name}`")
    return bool(locked_objects)  # Returns True if locked objects were found

def delete_bucket(profile_name, bucket_name, failures, locked_buckets, deleted_buckets):
    """Attempt to delete a bucket, logging only important failures."""
    print(f"Attempting to delete bucket `{bucket_name}`")
    s3 = get_s3_client(profile_name, force_delete=True)
    is_lock_enabled = False
    try:
        response = s3.get_object_lock_configuration(Bucket=bucket_name)
        is_lock_enabled = response.get('ObjectLockConfiguration', {}).get('ObjectLockEnabled') == 'Enabled'
        default_retention = response.get('ObjectLockConfiguration', {}).get('Rule', {}).get('DefaultRetention', {})
    except ClientError as e:
        pass
    
    if is_lock_enabled:
        print(f"Bucket `{bucket_name}` has object lock enabled with default retention {default_retention}. Trying without force delete...")
        s3 = get_s3_client(profile_name, force_delete=False)
        try:
            locked_buckets.append(bucket_name)
            delete_bucket_policy(s3, bucket_name)
            if delete_all_objects(s3, bucket_name):
                locked_buckets.append(bucket_name)
                return

            s3.delete_bucket(Bucket=bucket_name)
            deleted_buckets.append(bucket_name)
            return
        except ClientError as e:
            print(f"⚠️ Failed to check/delete policy for locked bucket {bucket_name}: {e}")
            failures.append((bucket_name, str(e)))
            return
    try:
        delete_bucket_policy(s3, bucket_name)

        s3.delete_bucket(Bucket=bucket_name)
        deleted_buckets.append(bucket_name)
    except (BotoCoreError, ClientError) as e:
        error_message = str(e)
        failures.append((bucket_name, error_message))
    print(f"Success deleting bucket `{bucket_name}`")

def _add_force_delete_header(model, params, request_signer, **kwargs):
    params["headers"]["X-Force-Container-Delete"] = "true"

def get_s3_client(profile_name, force_delete=False):
    """Get S3 client with optional force delete header."""
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')
    event_system = s3.meta.events
    if force_delete:
        event_system.register_first('before-call.s3.DeleteBucket', _add_force_delete_header)
    return s3

def purge_old_test_buckets(profile_name):
    """Main function to purge only test buckets older than 2 hours."""
    failures = []
    locked_buckets = []
    deleted_buckets = []

    old_test_buckets = list_old_test_buckets(profile_name)
    if not old_test_buckets:
        print(f"No old test buckets found (older than 2 hours) on profile {profile_name}.")
        return

    print(f"🔄 Starting purge of {len(old_test_buckets)} old test buckets on profile {profile_name}...")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_bucket = {
            executor.submit(delete_bucket, profile_name, bucket, failures, locked_buckets, deleted_buckets): bucket
            for bucket in old_test_buckets
        }
        for future in concurrent.futures.as_completed(future_to_bucket):
            future.result()

    print("\n🔍 **Purge Summary:**")

    if locked_buckets:
        print("\n🔒 Buckets skipped due to object lock:")
        for bucket in locked_buckets:
            print(f" - {bucket} (Protected by object lock)")

    if failures:
        print("\n❌ Buckets that failed to delete:")
        for bucket, reason in failures:
            print(f" - {bucket}: {reason}")

    if deleted_buckets:
        print(f"\n✅ {len(deleted_buckets)} old buckets successfully deleted on profile {profile_name}.")
    else:
        print(f"\n⚠️ No buckets were deleted on profile {profile_name}.")

if __name__ == "__main__":
    profile = sys.argv[1] if len(sys.argv) > 1 else input("Enter AWS profile name: ").strip()
    purge_old_test_buckets(profile)

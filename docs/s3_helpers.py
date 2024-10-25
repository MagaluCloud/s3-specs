import boto3
import datetime
import uuid

def print_timestamp():
    print(f'execution started at {datetime.datetime.now()}')

def create_s3_client(profile_name):
    session = boto3.Session(profile_name=profile_name)
    return session.client('s3')

def generate_unique_bucket_name(base_name="my-unique-bucket"):
    unique_id = uuid.uuid4().hex[:6]  # Short unique suffix
    return f"{base_name}-{unique_id}"

def delete_bucket_and_wait(s3_client, bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        print("already deleted by someone else")
        pass

    waiter = s3_client.get_waiter('bucket_not_exists')
    waiter.wait(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' confirmed as deleted.")

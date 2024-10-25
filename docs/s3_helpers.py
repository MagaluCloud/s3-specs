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

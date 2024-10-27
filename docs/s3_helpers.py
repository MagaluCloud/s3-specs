import boto3
import datetime
import uuid
import logging

def print_timestamp():
    logging.info(f'execution started at {datetime.datetime.now()}')

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
        logging.info("Bucket already deleted by someone else.")
        return

    waiter = s3_client.get_waiter('bucket_not_exists')
    waiter.wait(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' confirmed as deleted.")

def create_bucket_and_wait(s3_client, bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
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

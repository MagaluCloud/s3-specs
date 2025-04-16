import pytest
import logging
import subprocess
import boto3
import json
import uuid
import time
from botocore.exceptions import ClientError

@pytest.fixture
def profile_name_sa(test_params, profile_name):
    profiles = [profile['profile_name'] for profile in test_params['profiles']]
    
    logging.info(profiles)

    if f"{profile_name}-sa" not in profiles:
        pytest.skip("Service Account not provided")
    
    return f"{profile_name}-sa"

@pytest.fixture
def active_mgc_workspace_sa(profile_name_sa, mgc_path):
    # set the profile
    result = subprocess.run([mgc_path, "workspace", "set", profile_name_sa],
                            capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("This test requires an mgc profile name")

    logging.info(f"mcg workspace set stdout: {result.stdout}")
    return profile_name_sa

@pytest.fixture
def get_sa_infos(test_params):# Extrai a lista de perfis
    profiles = test_params['profiles']
    sa_info = None

    for profile in profiles:
        if "-sa" in profile['profile_name']:
            sa_info = profile

    logging.info(f"Service Account: {sa_info}")
    if not sa_info:
        pytest.skip("Service Account email key or tenant id not provided")
    
    return sa_info

@pytest.fixture
def bucket_with_one_object_and_bucket_policy(s3_client, bucket_with_one_object):
    def _add_policy(actions, principal, effect="Allow"):
        if isinstance(actions, str):
            actions = [actions]
        
        bucket_name, object_key, content = bucket_with_one_object

        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Principal": {
                       "MGC": principal
                    },
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [f"{bucket_name}/{object_key}"]
                }
            ]
        }

        logging.info(f"principal: {principal} effect {effect} action: {actions} resource {bucket_name}/{object_key}")
        logging.info(f"policy: {bucket_policy}")
        
        response = s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )
        
        logging.info(f"put policy response {response}")

        return bucket_name, object_key

    return _add_policy
import json
import pytest
import logging
import subprocess

@pytest.fixture
def fixture_public_bucket(s3_client, bucket_with_one_object):
    """Fixture que cria um upload multipart e retorna as informações necessárias para o teste."""

    bucket_name, object_key, _ = bucket_with_one_object

    response = s3_client.put_bucket_acl(
    Bucket=bucket_name,
    ACL="public-read"
    )
    
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    return bucket_name, object_key


@pytest.fixture
def fixture_private_bucket(s3_client, bucket_with_one_object):
    """Fixture que cria um upload multipart e retorna as informações necessárias para o teste."""

    bucket_name, object_key, _ = bucket_with_one_object

    response = s3_client.put_bucket_acl(
    Bucket=bucket_name,
    ACL="private"
    )
    
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    return bucket_name, object_key

@pytest.fixture
def profile_name_second(test_params, profile_name):
    profiles = [profile['profile_name'] for profile in test_params['profiles']]
    
    logging.info(profiles)

    if f"{profile_name}-second" not in profiles:
        pytest.skip("Second Account not provided")
    
    return f"{profile_name}-second"

@pytest.fixture
def active_mgc_workspace_second(profile_name_second, mgc_path):
    # set the profile
    result = subprocess.run([mgc_path, "workspace", "set", profile_name_second],
                            capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("This test requires an mgc profile name")

    logging.info(f"mcg workspace set stdout: {result.stdout}")
    return profile_name_second

def generate_policy(effect, principals, actions, resources):
    Statement = []
    Statement.append({
        'Effect': effect,
        'Principal': principals,
        'Action': actions,
        'Resource': resources,
    })
    return json.dumps(
        {
            'Version': '2012-10-17',
            'Statement': Statement,
        },
        separators=(',', ':'),
    )
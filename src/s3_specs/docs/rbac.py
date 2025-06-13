import os
import logging
from datetime import datetime,timezone
import time
import pytest
import boto3
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import run_example


pytestmark = [pytest.mark.rbac]

config = "../params/br-ne1.yaml"
config = os.getenv("CONFIG", config)

# INFO: These key pairs are hard coded because clients have to have correct
# permissioning in Magalu Cloud console.
key_pairs = [
    #TODO
]
clients = [
    boto3.Session(
            region_name="br-ne1",
            aws_access_key_id=access,
            aws_secret_access_key=secret,
        ).client('s3', endpoint_url="https://br-ne1-yel.magaluobjects.com/")
    for access, secret in key_pairs
]
test_cases = [
    (client, action, j>i)
    for j, client in enumerate(clients)
    for i, action in enumerate(['list_buckets', 'create_bucket'])
]
@pytest.mark.parametrize(
    's3_client, boto3_action, positive_result',
     test_cases,
     ids = [f"{item[1]},{item[0]}" for item in test_cases],
)
def test_rbac_permissions(s3_client, boto3_action, positive_result):
    kwargs = {}
    if boto3_action == 'create_bucket':
        bucket_name = 'test-' + str(int(time.time()))
        kwargs = {
            'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        }

    logging.info(f"call boto3_action:{boto3_action}, with args:{kwargs}")
    method = getattr(s3_client, boto3_action)
    try:
        response = method(**kwargs)
        logging.info(f"Method response:{response}")
        if not positive_result:
            pytest.fail("Expected exception not raised")
    except ClientError as e:
        logging.info(f"Method error response:{e.response}")
        if positive_result:
            pytest.fail("Should not raise exception")
    finally:
        if kwargs.get('Bucket') is not None:
            cleanup_bucket(s3_client, kwargs['Bucket'])


run_example(__name__, "test_rbac_permissions", config=config)


def cleanup_bucket(s3_client, bucket_name):
    try:
        # Listar e excluir todos os objetos
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        # Excluir o bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} removido com sucesso")
    except Exception as e:
        logging.warning(f"Erro ao limpar bucket {bucket_name}: {str(e)}")

import os
import logging
import time
import pytest
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import run_example


pytestmark = [pytest.mark.rbac, pytest.mark.skip_if_dev]

config = "../params/br-ne1.yaml"
config = os.getenv("CONFIG", config)


# INFO: These key pairs are hard coded because clients have to have correct
# permissioning in Magalu Cloud console.
@pytest.mark.parametrize(
    'rbac_s3_client, results',
    [
       (0, (False, False)),
       (1, (True, False)),
       (2, (True, True))
    ],
    indirect=['rbac_s3_client'],
    ids=[f'client[rbac{id+1}]' for id in range(3)]
)
@pytest.mark.parametrize(
    'boto3_action',
    ['list_buckets', 'create_bucket']
)
def test_rbac_permissions(rbac_s3_client, boto3_action, results):
    kwargs = {}

    positive_result = results[0]
    if boto3_action == 'create_bucket':
        positive_result = results[1]
        bucket_name = 'test-' + str(int(time.time()))
        kwargs = {
            'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        }

    logging.info(f"call boto3_action:{boto3_action}, with args:{kwargs}")
    method = getattr(rbac_s3_client, boto3_action)
    try:
        response = method(**kwargs)
        logging.info(f"Method response:{response}")
        if not positive_result:
            pytest.fail("Expected exception not raised")
    except ClientError as e:
        logging.info(f"Method error response:{e.response}")
        if positive_result:
            pytest.fail(f"Should not raise exception, raised: {e}")
    finally:
        if kwargs.get('Bucket') is not None:
            cleanup_bucket(rbac_s3_client, kwargs['Bucket'])


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

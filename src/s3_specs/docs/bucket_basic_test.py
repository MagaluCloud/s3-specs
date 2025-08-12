# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Operações básicas em bucket S3
#
# Os exemplos abaixo demonstram como criar, listar e deletar buckets na MagaluCloud usando a
# biblioteca boto3 de Python. Bem como a limitação dos nomes de bucket precisarem ser únicos.

# + {"tags": ["parameters"], "jupyter": {"source_hidden": true}}
config = "../../../params/br-ne1.yaml"

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
from s3_specs.docs.s3_helpers import run_example
from s3_specs.docs.tools.crud import fixture_multiple_buckets
import os
import secrets
import uuid
import botocore

pytestmark = [pytest.mark.basic, pytest.mark.homologacao, pytest.mark.quick]
config = os.getenv("CONFIG", config)
# -

# ## Criar um novo bucket
# Na boto3, um bucket pode ser criado com o comando `create_bucket`.
# O método recebe o nome do bucket pelo argumento `Bucket`.

# +
create_bucket_bucket_names = [
    f"test-create-bucket-{uuid.uuid4().hex[:15]}",
    f"test-create-bucket-{uuid.uuid4().hex[:22]}", # 62 chars length
]

@pytest.mark.parametrize(
    'bucket_name',
    create_bucket_bucket_names,
    ids=range(len(create_bucket_bucket_names)) # workaround for -n auto bug with long names
)
def test_create_bucket(s3_client, bucket_name):
    try:
        logging.info(bucket_name)
        response = s3_client.create_bucket(Bucket=bucket_name)

        response_status = response["ResponseMetadata"]["HTTPStatusCode"]
        assert response_status == 200, "Expected HTTPStatusCode 200 for successful bucket create."
        response_location = response["Location"]
        assert bucket_name in response_location, "Expected bucket name is not on the response."
        assert response_location == f"/{bucket_name}"
    finally:
        logging.info(f"Deleting bucket {bucket_name}")
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} deleted sucessfully")

run_example(__name__, "test_create_bucket", config=config)
# -

# ### Regras para nome de bucket
# Os nomes de buckets possuem algumas restrições como tamanho e presença de alguns caracteres,
# a lista completa de regras pode ser consultada nos links de referência ao fim deste documento.
# Abaixo alguns exemplos de nomes inválidos:

# +
invalid_bucket_names = [
    "test-bucket-with-Capital-Letters",
    "test-create-bucket-com-acentuação",
    "test-bucket-with-underscores_in_the_name",
    "test-bucket-with-name-ending-in-hyfen-",
    f"test-bucket-with-offensive-sensitive-words-caralho-{secrets.token_hex(10)}",
    f"test-create-bucket--{secrets.token_hex(22)}", # 63 chars is the maximum length allowed
]
@pytest.mark.parametrize(
    'bucket_name',
    invalid_bucket_names,
    ids=range(len(invalid_bucket_names)) # workaround for -n auto bug with long names
)
def test_create_bucket_invalid_name(s3_client, bucket_name):
    logging.info(bucket_name)
    # Attempt to permanently delete the post-lock object version and expect failure
    with pytest.raises((botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError)) as exc_info:
        response = s3_client.create_bucket(Bucket=bucket_name)
        logging.info(response)
    logging.info(exc_info)
    assert any(msg in str(exc_info.value) for msg in ["InvalidBucketName", "Invalid bucket name"])

run_example(__name__, "test_create_bucket_invalid_name", config=config)
# -

# ### Nomes de bucket devem ser únicos
#
# Another important restriction of bucket names is that they should be unique. So duplicate names
# will fail with errors even between different users.

# +
@pytest.mark.parametrize("multiple_s3_clients, client_index", [
    ({"number_clients": 2}, 0),
    ({"number_clients": 2}, 1),
], indirect=["multiple_s3_clients"])
@pytest.mark.skip_if_dev
def test_create_bucket_duplicate(multiple_s3_clients, existing_bucket_name, client_index):
    logging.info(existing_bucket_name)
    sessions = multiple_s3_clients
    s3_client = sessions[client_index] # either the bucket owner or a second user, see parametrize
    # Attempt to permanently delete the post-lock object version and expect failure
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        response = s3_client.create_bucket(Bucket=existing_bucket_name)
        logging.info(response)
    logging.info(exc_info)
    assert "BucketAlreadyExists" in str(exc_info.value)

run_example(__name__, "test_create_bucket_duplicate", config=config)
# -

# ## Listar buckets
#
# No boto3, é possível listar todos os buckets de uma conta, utilizando o método list_buckets

# +
@pytest.mark.skip("This is a very expensive operation that may result in timeout")
def test_list_all_buckets(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name
    logging.info(bucket_name)
    response = s3_client.list_buckets()
    logging.info(response)
    bucket_names = [ bucket["Name"] for bucket in response["Buckets"]]
    assert bucket_name in bucket_names, "Existing bucket not found on the listing."

run_example(__name__, "test_list_all_buckets", config=config)
# -

# ## Deletar bucket
#
# O método do boto3 para a remoção de um bucket vazio, sem policy e sem locking é o `delete_bucket`
# passando o nome do bucket como o argumento `Bucket`.

# +
def test_delete_bucket(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name
    response = s3_client.delete_bucket(Bucket=bucket_name)
    logging.info(response)
    response_status_code = response['ResponseMetadata']['HTTPStatusCode']
    assert response_status_code == 204, "Delete bucket status should be 204"

run_example(__name__, "test_delete_bucket", config=config)
# -

# ## Extra
#
# ## Listar buckets com um determinado prefixo
#
# Na Magalu Cloud, a funcionalidade de filtrar a listagem de buckets apenas por buckets que
# contenham um prefixo ainda não está funcional, então utilizar o argumento `Prefix` do
# médoto `list_buckets` não vai filtrar o resultado, a lista sempre voltará completa.
#
# Esta é uma **limitação conhecida** da implementação da MagaluCloud. Quando for sanada o exemplo
# abaixo poderá ser executado com sucesso:

# +
# @pytest.mark.skip(reason="Not yet implemented")
@pytest.mark.parametrize("fixture_multiple_buckets, list_buckets_kwargs", [
    ({'prefix': "test-multiple-foo-", "names": ["1", "2"]}, {"Prefix": 'test-multiple-foo-'}),
], indirect=["fixture_multiple_buckets"]) 
def test_list_prefixed_buckets(s3_client, fixture_multiple_buckets, list_buckets_kwargs):
    bucket_names = fixture_multiple_buckets
    logging.info(bucket_names)
    logging.info(list_buckets_kwargs)
    response = s3_client.list_buckets(**list_buckets_kwargs)
    prefix = list_buckets_kwargs["Prefix"]
    logging.info(response)
    response_bucket_names = [ bucket["Name"] for bucket in response["Buckets"]]
    assert all(name.startswith(prefix) for name in response_bucket_names), \
        f"Nem todos os buckets começam com o prefixo '{prefix}': {response_bucket_names}"

run_example(__name__, "test_list_prefixed_buckets", config=config)
# -

# ## Referências
# - [Magalu Cloud - Regras de nomes de bucket](https://docs.magalu.cloud/docs/storage/object-storage/additional-explanations/naming-rules/)
# - [create_bucket - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3control/client/create_bucket.html)
# - [list_buckets - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)
# - [delete_bucket - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html)

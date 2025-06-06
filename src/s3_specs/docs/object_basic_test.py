# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Operações básicas em objetos S3
#
# Os exemplos abaixo demonstram como criar, listar e deletar objects na MagaluCloud usando a
# biblioteca boto3 de Python.

# + {"tags": ["parameters"], "jupyter": {"source_hidden": true}}
config = "../../../params/br-ne1.yaml"

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
from s3_specs.docs.s3_helpers import run_example
import os
import secrets
import botocore

pytestmark = [pytest.mark.basic, pytest.mark.quick, pytest.mark.homologacao]
config = os.getenv("CONFIG", config)
# -

# ## Upload de objetos
# No boto3 a criação de um novo objeto pode ser feita com o comando `put_object`, os principais
# argumentos são `Bucket` para o nome do conteiner onde este objeto será armazenado, `Key` para
# o nome do objeto e `Body` para o conteúdo do objeto, como mostra o exemplo abaixo:

# +
put_object_key_names = [
    "simple_name.txt",
    "name/that/looks/like/a/path.foo",
    "name_with_acentuação and emojis👀👀👀 and spaces.jpg",
    secrets.token_hex(462), # a very long key name, 924 chars
]
@pytest.mark.parametrize('object_key', put_object_key_names, ids=range(len(put_object_key_names)))
def test_put_object(s3_client, existing_bucket_name, object_key):
    bucket_name = existing_bucket_name
    object_content = b'foo'
    logging.info(f"bucket_name={bucket_name}, object_content={object_content}, object_key={object_key}")

    response = s3_client.put_object(
        Key=object_key,
        Bucket=bucket_name,
        Body=object_content,
    )

    logging.info(response)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful put object."
    assert response["ETag"], "ETag value is expected in the response of a put object."

run_example(__name__, "test_put_object", config=config)
# -

# ## Head de objetos
# Algumas informações de um object podem ser obtidas com um request HEAD sem a necessidade do
# download do conteúdo todo. No boto3 o comando para este tipo de checagem é o `head_object`

# +
def test_head_object(s3_client, bucket_with_one_object):
    bucket_name, object_key, content = bucket_with_one_object
    logging.info(f"bucket_name={bucket_name}, content={content}, object_key={object_key}")

    response = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key,
    )

    logging.info(response)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful head object."
    assert response["ETag"], "ETag value is expected in the response of a head object."
    assert response["LastModified"], "LastModified value is expected in the response of a head object."
    assert response["ContentLength"], "ContentLength value is expected in the response of a head object."
    assert response["ContentLength"] == len(content), "ContentLength value is expected to be the same as object length"

run_example(__name__, "test_head_object", config=config)
# -

# ## Listagem de objetos
# Para obter a lista de objetos de um bucket, o comando na boto3 é o `list_objects`

# +
def test_list_objects(s3_client, bucket_with_many_objects):
    bucket_name, object_prefix, content, object_key_list = bucket_with_many_objects
    logging.info(f"bucket_name={bucket_name}, object_key_list={object_key_list}")

    response = s3_client.list_objects(Bucket=bucket_name)

    logging.info(response)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful list object."
    response_object_key_list = [obj["Key"] for obj in response["Contents"]]
    assert response_object_key_list == object_key_list
    response_content_size_list = [obj["Size"] for obj in response["Contents"]]
    assert response_content_size_list == [len(content)] * len(object_key_list)

run_example(__name__, "test_list_objects", config=config)
# -

# ## Download de objetos
# O download de um objeto no boto3 é feito com o comando `get_object`

# +
def test_get_object(s3_client, bucket_with_one_object):
    bucket_name, object_key, content = bucket_with_one_object
    logging.info(f"bucket_name={bucket_name}, content={content}, object_key={object_key}")

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key,
    )

    logging.info(response)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful get object."
    object_body = response["Body"].read()
    assert object_body == content, "Expected object content to be the same."

run_example(__name__, "test_get_object", config=config)
# -

# ## Deleção de objetos
# No boto3, o método para a remoção de objetos em buckets sem locking, versionamento ou policy é
# o `delete_object`

# +
def test_delete_object(s3_client, bucket_with_one_object):
    bucket_name, object_key, _content = bucket_with_one_object
    logging.info(f"bucket_name={bucket_name}, object_key={object_key}")

    response = s3_client.delete_object(
        Bucket=bucket_name,
        Key=object_key,
    )

    logging.info(response)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 204, "Expected HTTPStatusCode 204 for successful delete object."

run_example(__name__, "test_delete_object", config=config)
# -

# ## Referências
# - [put_object - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html)
# - [head_object - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html)
# - [get_object - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html)
# - [list_objects - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects.html)
# - [delete_object - Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html)

# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Versionamento

# O que é o Versionamento?

# O versionamento é uma funcionalidade oferecida por sistemas de armazenamento de objetos, como o Magalu Cloud, 
# que permite manter múltiplas versões de um mesmo objeto dentro de um bucket. Isso significa que, ao habilitar o versionamento,
# você pode recuperar, restaurar ou excluir versões anteriores de arquivos, garantindo uma trilha de auditoria completa 
# e prevenindo perdas acidentais de dados.

# No Magalu Cloud, o versionamento é inspirado no funcionamento do Amazon S3, 
# oferecendo um mecanismo robusto de controle sobre a evolução de objetos dentro de um bucket.

# + {"jupyter": {"source_hidden": true}}
import logging
import pytest
from s3_helpers import run_example
from botocore.exceptions import ClientError

# + tags=["parameters"]
config = "../params/br-se1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
pytestmark = pytest.mark.bucket_versioning

# Este teste tem como objetivo verificar a exclusão bem-sucedida de um objeto da lista padrão de objetos 
# em um bucket com versionamento habilitado. 
# Além disso, valida que o histórico de versões mantém ambas as versões do objeto (v1 e v2), 
# mesmo após a exclusão do objeto de forma padrão.
def test_delete_object_with_versions(s3_client, versioned_bucket_with_one_object):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"second version of this object"
    )

    response = s3_client.delete_object(
        Bucket=bucket_name,
        Key = object_key
    )

    response_status_code = response['ResponseMetadata'].get("HTTPStatusCode")
    logging.info("Response status code: %s", response_status_code)

    assert response_status_code == 204, "Expected status code 204"

    list_objects_response = s3_client.list_objects(
        Bucket=bucket_name,
    )

    objects = list_objects_response.get("Contents")
    logging.info("List Objects Response: %s", list_objects_response)
    logging.info("Objects: %s", objects)

    assert objects is None, "Expected any object in list"

    list_object_versions_response = s3_client.list_object_versions(
        Bucket=bucket_name,
    )

    num_versions = len(list_object_versions_response.get('Versions'))

    logging.info(f"Qtd versions: {num_versions}")

    assert num_versions == 2, "Expected bucket has 2 versions"



run_example(__name__, "test_delete_object_with_versions", config=config)

# Este teste tem como objetivo verificar que um bucket versionado contendo objetos (com versões) 
# não pode ser excluído diretamente. 
# O teste tenta excluir o bucket e espera que seja levantada uma exceção do tipo `ClientError` 
# com o código de erro "BucketNotEmpty", indicando que o bucket ainda contém objetos, 
# mesmo em um cenário de versionamento.
def test_delete_bucket_with_objects_with_versions(s3_client, versioned_bucket_with_one_object):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"v2"
    )

    with pytest.raises(ClientError, match="BucketNotEmpty") as exc_info:
        s3_client.delete_bucket(
            Bucket=bucket_name,
        )

    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code == "BucketNotEmpty"
    
run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

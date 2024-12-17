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

import logging
import pytest
from s3_helpers import run_example
from botocore.exceptions import ClientError

# + tags=["parameters"]
config = "../params/br-se1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}


pytestmark = pytest.mark.bucket_versioning

def test_delete_object_with_versions(s3_client, versioned_bucket_with_one_object):
    bucket_name, object_key, _ = versioned_bucket_with_one_object

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"v2"
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



run_example(__name__, "test_delete_object_with_versions", config=config)


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

    # Opcional: verifique detalhes da exceção
    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code == "BucketNotEmpty"

    error_message = exc_info.value.response["Error"]["Message"]
    assert error_message == "The bucket you tried to delete is not empty"
run_example(__name__, "test_delete_bucket_with_objects_with_versions", config=config)

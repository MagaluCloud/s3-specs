# ---
# jupyter:
#   jupytext:
#     cell_metadata_json: true
#     notebook_metadata_filter: language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.12.7
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
from s3_specs.docs.s3_helpers import run_example
from botocore.exceptions import ClientError
from s3_specs.docs.utils.versioning import fixture_multipart_upload

# + {"tags": ["parameters"]}
config = "../params/br-se1.yaml"

# +
pytestmark = [pytest.mark.bucket_versioning, pytest.mark.quick, pytest.mark.homologacao, pytest.mark.only_run_in_region("br-se1", "us-east-1")]

# ## Deletar objeto com duas versões em uma bucket com versionamento
# Este teste tem como objetivo verificar a exclusão bem-sucedida de um objeto da lista padrão de objetos 
# em um bucket com versionamento habilitado. 
# Além disso, valida que o histórico de versões mantém ambas as versões do objeto (v1 e v2), 
# mesmo após a exclusão do objeto de forma padrão.
# -

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

# ## Deletar um bucket com versionamento com um objeto
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
# -

# ### Upload Multipart Versionado

# Este teste valida o processo de upload multipart em um bucket com versionamento habilitado. O
# teste simula o envio de várias partes de um objeto para o S3 e, em seguida, completa o upload
# multipart. Ele verifica se todas as partes foram enviadas corretamente e se o objeto finalizado
# possui a classe de armazenamento esperada, além de garantir que o versionamento do objeto
# seja mantido após a conclusão do upload.

# +
def test_multipart_upload_versioned(s3_client, fixture_multipart_upload):
    # Recebe as variáveis retornadas pela fixture
    bucket_name, object_key, upload_id, part_bytes = fixture_multipart_upload

    # Verifica a classe de armazenamento do objeto
    head = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key
    )
    logging.info("Storage Class: %s", head.get("StorageClass"))
    assert head.get("StorageClass") == "STANDARD"

run_example(__name__, "test_multipart_upload_versioned_with_cold_storage_class", config=config)
# -

# ### Excluir Versão 1 de Objeto com Classe de Armazenamento Cold

# Este teste valida o processo de exclusão de uma versão específica de um objeto que está armazenado
# em uma classe de armazenamento "GLACIER_IR". O teste simula o envio de uma nova versão de um objeto
# (versão 2), seguido pela exclusão da versão 1. Em seguida, ele verifica se a versão 1 foi realmente
# excluída e se não pode ser acessada, enquanto a versão 2 permanece intacta e acessível. Isso assegura
# que o gerenciamento de versões e o comportamento da classe de armazenamento "Cold" estão funcionando corretamente.

# +
def test_delete_object_version1_cold_storage_class(s3_client, versioned_bucket_with_one_object_cold_storage_class):
    bucket_name, object_key, version_v1 = versioned_bucket_with_one_object_cold_storage_class

    response_v2 = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=b"v2",
        StorageClass='GLACIER_IR'
    )
    version_v2 = response_v2["VersionId"]
    logging.info(f"Version v2 uploaded with VersionId: {version_v2}")

    s3_client.delete_object(
        Bucket=bucket_name,
        Key=object_key,
        VersionId=version_v1
    )
    logging.info(f"Version v1 deleted, VersionId: {version_v1}")

    response_versions = s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=object_key
    )

    deleted_version_v1 = any(version["VersionId"] == version_v1 for version in response_versions.get("Versions", []))
    assert not deleted_version_v1, f"Version v1 with VersionId {version_v1} should not be present after deletion"

    logging.info(f"Version v1 successfully deleted from the list of versions.")

    with pytest.raises(ClientError) as exc_info:
        s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key,
            VersionId=version_v1
        )
    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code == "NoSuchVersion" or "NoSuchKey", f"Expected 'NoSuchVersion', got {error_code}" ## NoSuchKey is a valid error code for MGC

    response_v2_get = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key,
        VersionId=version_v2
    )
    assert response_v2_get["Body"].read() == b"v2", "Expected content for v2 to be 'v2'"
    logging.info(f"Version v2 is still available with VersionId: {version_v2}")

run_example(__name__, "test_delete_object_version1_cold_storage_class", config=config)
# +

# ### Excluir Versão 2 de Objeto com Classe de Armazenamento Cold

# Este teste valida a exclusão de uma versão específica de um objeto armazenado em uma classe de armazenamento
# "GLACIER_IR". O teste simula o envio de uma nova versão de um objeto (versão 2), seguida pela exclusão dessa
# versão (versão 2). Em seguida, o teste verifica que a versão 2 foi excluída com sucesso e não pode ser acessada,
# enquanto a versão anterior (versão 1) permanece intacta e acessível. Isso assegura que o gerenciamento de versões
# e o comportamento da classe de armazenamento "Cold" funcionam corretamente.

# +
def test_delete_object_version2_cold_storage_class(s3_client, versioned_bucket_with_one_object_cold_storage_class):
    bucket_name, object_key, version_v1 = versioned_bucket_with_one_object_cold_storage_class

    response_v2 = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=b"v2",
        StorageClass='GLACIER_IR'
    )
    version_v2 = response_v2["VersionId"]
    logging.info(f"Version v2 uploaded with VersionId: {version_v2}")

    s3_client.delete_object(
        Bucket=bucket_name,
        Key=object_key,
        VersionId=version_v2
    )
    logging.info(f"Version v2 deleted, VersionId: {version_v2}")

    response_versions = s3_client.list_object_versions(
            Bucket=bucket_name,
            Prefix=object_key
        )

    deleted_version_v2 = any(version["VersionId"] == version_v2 for version in response_versions.get("Versions", []))
    assert not deleted_version_v2, f"Version v2 with VersionId {version_v2} should not be present after deletion"

    logging.info(f"Version v2 successfully deleted from the list of versions.")

    with pytest.raises(ClientError) as exc_info:
        s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key,
            VersionId=version_v2
        )
    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code == "NoSuchVersion" or "NoSuchKey", f"Expected 'NoSuchVersion', got {error_code}" ## NoSuchKey is a valid error code for MGC

    response_v1_get = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key,
        VersionId=version_v1
    )
    assert response_v1_get["Body"].read() == b"v1", "Expected content for v1 to be 'v1'"
    logging.info(f"Version v1 is still available with VersionId: {version_v1}")

run_example(__name__, "test_delete_object_version2_cold_storage_class", config=config)

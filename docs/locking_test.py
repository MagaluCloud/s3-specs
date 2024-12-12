# ---
# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # Object Locking
# 
# A funcionalidade de **Object Locking** no S3 permite bloquear versões individuais de objetos, 
# impedindo sua modificação ou exclusão durante um período especificado.
#
# Isto é usado para garantir **conformidade** (compliance) com requisitos legais ou simplesmente
# garantir uma proteção extra contra modificações ou exclusão.
#
# ## Pontos importantes
# 
# - Object Locking só pode ser utilizado em buckets com versionamento habilitado
# - A configuração do periodo de retenção, quando adicionada como regra do bucket, só será aplicada
# em novos objetos, incluidos após a configuração
# - Uma configuração de locking existir, não previne deletes simples (delete marker), pois estes
# não removem dados, a trava é apenas para deletes permanentes (delete com a version ID).
 
# + tags=["parameters"]
config = "../params/aws-east-1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import boto3
import os
import botocore
import pytest
import logging
from datetime import datetime, timedelta
from s3_helpers import (
    run_example,
    cleanup_old_buckets,
    generate_unique_bucket_name,
    create_bucket_and_wait,
    put_object_and_wait,
    cleanup_old_buckets,
)
config = os.getenv("CONFIG", config)
# -
pytestmark = pytest.mark.locking

# ### Configuração de Object Locking em Bucket Versionado
# 
# A configuração de uma trava em um bucket deve ser feita em um bucket com versionamento habilitado
# e é setada com o comando **put_object_lock_configuration**
#
# Para os exemplos abaixo, vamos utilizar um bucket versionado com dois objetos, um de antes da entrada
# da configuração de lock e outro de depois, quando uma regra de retenção padráo já foi definida.
# 
# Isto facilitará a demonstração de que regras de retenção do buckect só se aplicam às novas versões de objetos.

# +
@pytest.fixture
def versioned_bucket_with_lock_config(s3_client, versioned_bucket_with_one_object, lock_mode):
    bucket_name, first_object_key, first_version_id = versioned_bucket_with_one_object

    # Configure Object Lock on the bucket
    lock_config = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {
            "DefaultRetention": {
                "Mode": lock_mode,
                "Days": 1
            }
        }
    }
    response = s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=lock_config
    )
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful lock configuration."
    logging.info(f"Bucket '{bucket_name}' locked with mode {lock_mode}. Status: {response_status}")

    # Upload another object after lock configuration
    second_object_key = "post-lock-object.txt"
    post_lock_content = b"Content for object after lock configuration"
    second_version_id = put_object_and_wait(s3_client, bucket_name, second_object_key, post_lock_content)
    logging.info(f"Uploaded post-lock object: {bucket_name}/{second_object_key} with version ID {second_version_id}")

    # Yield details for tests to use
    yield bucket_name, first_object_key, second_object_key, first_version_id, second_version_id

    # cleanup whatever is possible given the lock mode
    cleanup_old_buckets(s3_client, bucket_name, lock_mode)
# -

# ### Remoção de objetos em um bucket com lock configuration

# #### Simple Delete
# Em um bucket versionado, um delete simples sem o id da versão do objeto não exclui dados, apenas
# adiciona um marcador (delete marker), esta operação pode ocorrer independentemente se o bucket
# possui ou não uma configuração de locking.

# +
def test_simple_delete_with_lock(versioned_bucket_with_lock_config, s3_client):
    bucket_name, first_object_key, second_object_key, _, _ = versioned_bucket_with_lock_config

    # Simple delete (without specifying VersionId), adding a delete marker
    logging.info(f"Attempting simple delete (delete marker) on pre-lock object: {bucket_name}/{first_object_key}")
    response = s3_client.delete_object(Bucket=bucket_name, Key=first_object_key)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 204, "Expected HTTPStatusCode 204 for successful simple delete."
    logging.info(f"Simple delete (delete marker) added successfully for object '{first_object_key}'.")

    # Simple delete the locked (second object)
    logging.info(f"Attempting simple delete (delete marker) on object: {bucket_name}/{second_object_key}")
    response = s3_client.delete_object(Bucket=bucket_name, Key=second_object_key)
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 204, "Expected HTTPStatusCode 204 for successful simple delete."
    logging.info(f"Simple delete (delete marker) added successfully for object '{second_object_key}'.")

run_example(__name__, "test_simple_delete_with_lock", config=config)
# -

# #### Permanent Delete
#
# Já um delete permanente, que específica a versão do objeto, é afetado pela configuração de
# locking e retorna um erro de `AccessDenied` se for aplicado a um objeto que tenha sido criado 
# **após** a entrada da configuração e que esteja ainda em período de retenção.
#
# Versões de objetos anteriores à configuração de uma retenção padrão podem ser deletadas 
# permanentemente.

# +
def test_delete_object_after_locking(versioned_bucket_with_lock_config, s3_client):
    bucket_name, first_object_key, second_object_key, first_version_id, second_version_id = versioned_bucket_with_lock_config

    # Perform a permanent delete on the pre-lock object version (should succeed due to no retention)
    delete_response = s3_client.delete_object(Bucket=bucket_name, Key=first_object_key, VersionId=first_version_id)
    delete_response_status = delete_response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info(f"delete response status: {delete_response_status}")

    # Attempt to permanently delete the post-lock object version and expect failure
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.delete_object(Bucket=bucket_name, Key=second_object_key, VersionId=second_version_id)

    # Verify AccessDenied for the newly uploaded locked object
    error_code = exc_info.value.response['Error']['Code']
    assert error_code == "AccessDenied", f"Expected AccessDenied, got {error_code}"
    logging.info(f"Permanent deletion blocked as expected for new locked object '{second_object_key}' with version ID {second_version_id}")

run_example(__name__, "test_delete_object_after_locking", config=config)
# -


# ### Conferindo a existência de uma configuração de tranca no bucket
#
# É possível consultar se um **bucket** possui uma configuração de lock por meio do comando
# **get_object_lock_configuration**

# +
def test_verify_object_lock_configuration(bucket_with_lock, s3_client, lock_mode):
    bucket_name = bucket_with_lock

    # Retrieve and verify the applied bucket-level Object Lock configuration
    logging.info("Retrieving Object Lock configuration from bucket...")
    applied_config = s3_client.get_object_lock_configuration(Bucket=bucket_name)
    assert applied_config["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled", "Expected Object Lock to be enabled."
    assert applied_config["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"] == lock_mode, f"Expected retention mode to be {lock_mode}."
    assert applied_config["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Days"] == 1, "Expected retention period of 1 day."
    logging.info("Verified that Object Lock configuration was applied as expected.")
run_example(__name__, "test_verify_object_lock_configuration", config=config)
# -

# ### Conferindo a politica de retenção de objetos específicos
#
# É possível consultar as regras de retenção para objetos novos, criados após a configuração de
# uma regra padrão por meio dos comando **get_object_retention** e **head_object**.
# Objetos pre-existentes, de antes da configuração do bucket não exibem estas informações.

# +
def test_verify_object_retention(versioned_bucket_with_lock_config, s3_client, lock_mode):
    bucket_name, first_object_key, second_object_key, _, _ = versioned_bucket_with_lock_config

    # Objects from before the config don't have retention data
    logging.info(f"Fetching data of the pre-existing object with a head request...")
    head_response = s3_client.head_object(Bucket=bucket_name, Key=first_object_key)
    assert not head_response.get('ObjectLockRetainUntilDate'), 'Expected lock ending date to be unset.'
    assert not head_response.get('ObjectLockMode'), 'Expected lock mode to be unset'
    logging.info(f"Retention data not present on the pre-existing object as expected.")

    # Use get_object_retention to check object-level retention details
    logging.info("Retrieving object retention details...")
    retention_info = s3_client.get_object_retention(Bucket=bucket_name, Key=second_object_key)
    assert retention_info["Retention"]["Mode"] == lock_mode, f"Expected object lock mode to be {lock_mode}."
    logging.info(f"Retention verified as applied with mode {retention_info['Retention']['Mode']} "
          f"and retain until {retention_info['Retention']['RetainUntilDate']}.")

    # Use head_object to check retention details
    logging.info("Fetching data of the new object with a head request...")
    head_response = s3_client.head_object(Bucket=bucket_name, Key=second_object_key)
    assert head_response['ObjectLockRetainUntilDate'], 'Expected lock ending date to be present.'
    assert head_response['ObjectLockMode'] == lock_mode, f"Expected lock mode to be {lock_mode}"
    logging.info(f"Retention verified as applied with mode {head_response['ObjectLockMode']} "
          f"and retain until {head_response['ObjectLockRetainUntilDate']}.")
run_example(__name__, "test_verify_object_retention", config=config,)
# -

# ### Configuração de Object Locking em Bucket Não-Versionado
#
# Para que o Object Lock funcione, o bucket deve estar configurado com **versionamento** habilitado,
# pois o bloqueio opera no nível de versão. Aplicar uma configuração de object locking em um
# bucket comum (não versionado), deve retornar um erro do tipo `InvalidBucketState`.

# +
def test_configure_bucket_lock_on_regular_bucket(s3_client, existing_bucket_name, lock_mode):
    # Set up Bucket Lock configuration
    bucket_lock_config = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {
            "DefaultRetention": {
                "Mode": lock_mode,
                "Days": 1
            }
        }
    }

    # Try applying the Object Lock configuration and expect an error
    logging.info("Attempting to apply Object Lock configuration on a non-versioned bucket...")
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.put_object_lock_configuration(
            Bucket=existing_bucket_name,
            ObjectLockConfiguration=bucket_lock_config
        )

    # Verify that the correct error was raised
    assert "InvalidBucketState" in str(exc_info.value), "Expected InvalidBucketState error not raised."
    logging.info("Correctly raised InvalidBucketState error for non-versioned bucket.")

run_example(__name__, "test_configure_bucket_lock_on_regular_bucket", config=config)
# -


# ## Referências
# - [Amazon S3 Object Lock Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) - Visão Geral sobre Object Lock no Amazon S3
# - [put_object_lock_configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object_lock_configuration.html) - Configurar Object Lock em um bucket
# - [get_object_lock_configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object_lock_configuration.html) - Recuperar configuração de Object Lock de um bucket
# - [Why can I delete objects even after I turned on Object Lock for my Amazon S3 bucket?](https://repost.aws/knowledge-center/s3-object-lock-delete) - Detalhamento de como deletar e gerenciar retenção e legal hold em objetos S3

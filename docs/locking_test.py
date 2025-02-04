# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Object Locking
# 
# A funcionalidade de **Object Locking** no S3 permite bloquear versões individuais de objetos, 
# impedindo sua modificação ou exclusão durante um período especificado.
#
# Isto é usado para garantir **conformidade** (compliance) com requisitos legais ou simplesmente
# garantir uma proteção extra contra modificações ou exclusões.
#
# ## Pontos importantes
# 
# - Object Locking só pode ser utilizado em buckets com versionamento habilitado
# - A configuração do periodo de retenção, quando adicionada como regra do bucket, só será aplicada
# em novos objetos, incluidos após a configuração
# - Uma configuração de locking existir, não previne deletes simples (delete marker), pois estes
# não removem dados, a trava é apenas para deletes permanentes (delete com a version ID).
 
# + tags=["parameters"]
config = "../params/br-se1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import boto3
import os
import json
import botocore
import pytest
import time
import logging
from datetime import datetime, timedelta, timezone
from s3_helpers import (
    run_example,
    cleanup_old_buckets,
    generate_unique_bucket_name,
    create_bucket_and_wait,
    put_object_and_wait,
    cleanup_old_buckets,
    replace_failed_put_without_version,
    get_object_lock_configuration_with_determination,
    get_object_retention_with_determination,
)
from utils.locking import bucket_with_lock_enabled

config = os.getenv("CONFIG", config)
pytestmark = pytest.mark.locking
# -

# ### Criando um novo bucket, já com locking habilitado
#
# No boto3 a função `create_bucket` permite que um novo bucket já seja criado com a opção de
# suporte a _Object Locking_ habilitada, para tanto o argumento booleano
# `ObjectLockEnabledForBucket` deve ser passado, como mostra o exemplo a seguir.

# +
def test_create_bucket_with_lock_enabled(bucket_name, s3_client):
    # create bucket with lock enabled
    create_bucket_response = s3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    # check that the bucket has the ability to lock
    applied_config = s3_client.get_object_lock_configuration(Bucket=bucket_name)
    assert applied_config["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled", "Expected Object Lock to be enabled."

    # check that the bucket is also versioned
    get_bucket_versioning_response = s3_client.get_bucket_versioning(Bucket=bucket_name)
    assert get_bucket_versioning_response['Status'] == "Enabled"

run_example(__name__, "test_create_bucket_with_lock_enabled", config=config)
# -

# ### Criando um novo objeto, já com uma configuração de trava
#
# No boto3 a função `put_object` permite que um novo objeto criado em bucket com locking habilitado
# suba com uma trava habilitada e uma data de validade para o período de retenção, para isto
# os argumentos `ObjectLockMode` e `ObjectLockRetainUntilDate` devem ser passados, como mostra o
# exemplo a seguir

# +
def test_create_object_with_lock_enabled(bucket_with_lock_enabled, s3_client, lock_mode):
    bucket_name = bucket_with_lock_enabled

    # upload a new object, with a specific lock configuration with the retention rules to use
    object_key="test-object"
    object_body="create object with lock test object content"
    retain_until_date = datetime.now(timezone.utc) + timedelta(days=1)
    put_object_response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=object_body,
        ObjectLockMode=lock_mode,
        ObjectLockRetainUntilDate=retain_until_date,
    )

    # assert that the put object succeeded
    response_status = put_object_response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful object upload"

    # assert that the object has a retention set
    retention_info = s3_client.get_object_retention(Bucket=bucket_name, Key=object_key)
    assert retention_info["Retention"]["Mode"] == lock_mode, f"Expected object lock mode to be {lock_mode}."


run_example(__name__, "test_create_object_with_lock_enabled", config=config)
# -

# ### Configuração de Object Locking em Bucket Versionado Existente
# 
# A configuração de uma trava em um bucket deve ser feita em um bucket com versionamento habilitado
# e é setada com o comando **put_object_lock_configuration**
#
# Para os exemplos abaixo, vamos utilizar um bucket versionado com dois objetos, um de antes da entrada
# da configuração de lock e outro de depois, quando uma regra de retenção padráo já foi definida.
# 
# Isto facilitará a demonstração de que regras de retenção do bucket só se aplicam às novas versões de objetos.

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
    logging.info(f"calling put_object_lock_configuration...")
    response = s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=lock_config
    )
    logging.info(f"put_object_lock_configuration response: {response}")
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful lock configuration."
    logging.info(f"Bucket '{bucket_name}' locked with mode {lock_mode}. Status: {response_status}")

    # wait for the lock configuration to return on get calls
    applied_config = get_object_lock_configuration_with_determination(s3_client, bucket_name)
    assert applied_config["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled", "Expected Object Lock to be enabled."
    logging.info(f"A lock configuration exists on te bucket, if you trust the return of get_object_lock_configuration call.")

    # Upload another object after lock configuration
    second_object_key = "post-lock-object.txt"
    post_lock_content = b"Content for object after lock configuration"
    logging.info(f"Making a put_object on a bucket that is supposed to have lock configuration...")
    second_version_id = put_object_and_wait(s3_client, bucket_name, second_object_key, post_lock_content)
    if not second_version_id:
        second_version_id, second_object_key = replace_failed_put_without_version(s3_client, bucket_name, second_object_key, post_lock_content)

    assert second_version_id, "Setup failed, could not get VersionId from put_object in versioned bucket"

    logging.info(f"Uploaded post-lock object: {bucket_name}/{second_object_key} with version ID {second_version_id}")

    # wait for the lock configuration to return on get object retention calls
    retention_info = get_object_retention_with_determination(s3_client, bucket_name, second_object_key)
    assert retention_info["Retention"]["Mode"] == lock_mode, f"Expected object lock mode to be {lock_mode}."
    logging.info(f"Retention verified as applied with mode {retention_info['Retention']['Mode']} "
          f"and retain until {retention_info['Retention']['RetainUntilDate']}.")

    # Yield details for tests to use
    yield bucket_name, first_object_key, second_object_key, first_version_id, second_version_id

    # cleanup whatever is possible given the lock mode
    cleanup_old_buckets(s3_client, bucket_name, lock_mode)
# -

# ### Remoção de objetos em um bucket com lock configuration

# #### Simple Delete
# Em um bucket versionado, um delete simples sem o id da versão do objeto não exclui dados, apenas
# adiciona um marcador (delete marker), esta operação pode ocorrer independentemente de se o bucket
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
    # the commented line below is the boto3 command to get object lock configuration, we use a helper function to account for MagaluCloud eventual consistency
    # applied_config = s3_client.get_object_lock_configuration(Bucket=bucket_name)
    applied_config = get_object_lock_configuration_with_determination(s3_client, bucket_name)
    assert applied_config["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled", "Expected Object Lock to be enabled."
    assert applied_config["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"] == lock_mode, f"Expected retention mode to be {lock_mode}."
    assert applied_config["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Days"] == 1, "Expected retention period of 1 day."
    logging.info("Verified that Object Lock configuration was applied as expected.")
run_example(__name__, "test_verify_object_lock_configuration", config=config)
# -

# ### Conferindo a politica de retenção de objetos específicos
#
# É possível consultar as regras de retenção para objetos novos, criados após a configuração de
# uma regra padrão por meio do comando **get_object_retention**
# Objetos pre-existentes, de antes da configuração do bucket não exibem estas informações.

# +
def test_verify_object_retention(versioned_bucket_with_lock_config, s3_client, lock_mode):
    bucket_name, first_object_key, second_object_key, _, _ = versioned_bucket_with_lock_config

    # Objects from before the config don't have retention data
    logging.info(f"Fetching data of the pre-existing object with a get_object_retention request...")
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.get_object_retention(Bucket=bucket_name, Key=first_object_key)
    # Verify that the correct error was raised
    assert "NoSuchObjectLockConfiguration" in str(exc_info.value), "Expected NoSuchObjectLockConfiguration error not raised."
    logging.info(f"Retention data not present on the pre-existing object as expected.")

    # Use get_object_retention to check object-level retention details
    logging.info("Fetching data of the post-lock-config with a get_object_retention request...")
    retention_info = get_object_retention_with_determination(s3_client, bucket_name, second_object_key)
    assert retention_info["Retention"]["Mode"] == lock_mode, f"Expected object lock mode to be {lock_mode}."
    logging.info(f"Retention verified as applied with mode {retention_info['Retention']['Mode']} "
          f"and retain until {retention_info['Retention']['RetainUntilDate']}.")

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

# ## Outros exemplos

# ### Aumentar o tempo de trava (retention date) em um objeto
#
# O período de locking de um objeto específico pode ser aumentado individualmente com o uso do
# método `put_object_retention` e uma data para o argumento RetainUntilDate que seja superior ã
# data de retenção atual. Tentar utilizar uma data menor que a data de retenção atual deve falhar
# com um erro de `InvalidArgument` como demonstra o teste abaixo:

# +
def test_put_object_retention(versioned_bucket_with_lock_config, s3_client, lock_mode, lock_wait_time):
    bucket_name, _, second_object_key, _, _ = versioned_bucket_with_lock_config

    # Use get_object_retention to check object-level retention details
    logging.info("Retrieving object retention details...")
    # retention_info = s3_client.get_object_retention(Bucket=bucket_name, Key=second_object_key)
    retention_info = get_object_retention_with_determination(s3_client, bucket_name, second_object_key)
    assert retention_info["Retention"]["Mode"] == lock_mode, f"Expected object lock mode to be {lock_mode}."
    logging.info(f"Retention verified as applied with mode {retention_info['Retention']['Mode']} "
          f"and retain until {retention_info['Retention']['RetainUntilDate']}.")

    # Increase the RetainUntilDate in one day
    new_retain_until_date = retention_info['Retention']['RetainUntilDate'] + timedelta(days=1)
    logging.info(f"new retention date for {second_object_key} will be {new_retain_until_date}")

    retention_update_response = s3_client.put_object_retention(
        Bucket=bucket_name,
        Key=second_object_key,
        Retention={ "RetainUntilDate": new_retain_until_date }
    )
    logging.info(f"put_object_retention response 1: {retention_update_response}")
    assert retention_update_response["ResponseMetadata"]["HTTPStatusCode"] == 200, "Expected HTTPStatusCode 200 for successful lock configuration."
    new_retention_info = get_object_retention_with_determination(s3_client, bucket_name, second_object_key)
    logging.info(f"new retention date for {second_object_key} is {new_retention_info['Retention']['RetainUntilDate']}")
    assert new_retention_info['Retention']['RetainUntilDate'] == new_retain_until_date

    # Decrease the RetainUntilDate to somethin in the past (invalid)
    invalid_retain_until_date = retention_info['Retention']['RetainUntilDate'] - timedelta(days=2)
    logging.info(f"invalid retention date for {second_object_key} cant be {new_retain_until_date}")

    # wait for the first lock change to be effective
    wait_time = lock_wait_time
    logging.info(f"Put object retention might take time to propagate. Wait more {wait_time} seconds")
    time.sleep(wait_time)

    # Attempt to put a date that is not in the future as object retention date
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        retention_update_response = s3_client.put_object_retention(
            Bucket=bucket_name,
            Key=second_object_key,
            Retention={ "RetainUntilDate": invalid_retain_until_date }
        )
    logging.info(f"exc_info.value.response {exc_info.value.response}")
    error_code = exc_info.value.response['Error']['Code']
    assert error_code == "InvalidArgument", f"Expected InvalidArgument, got {error_code}"

    # Double check that the retention date is still the same
    latest_retention_info = get_object_retention_with_determination(s3_client, bucket_name, second_object_key)
    logging.info(f"latest retention date for {second_object_key} should continue to be {latest_retention_info['Retention']['RetainUntilDate']}")
    assert latest_retention_info['Retention']['RetainUntilDate'] == new_retain_until_date
run_example(__name__, "test_put_object_retention", config=config,)
# -

# ### Impedir a modificação do tempo de trava em um objeto, por meio de uma Bucket Policy
#
# Abaixo um exemplo de política de bucket para impedir que a action `put_object_retention` seja
# utilizada em objetos de um determinado bucket. Na sintaxe de bucket policy o nome desta action
# é `s3:PutObjectRetention`

# +
policy_template = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObjectRetention",
            "Resource": "{bucket_name}/*"
        }
    ]
}
# -

# + {"jupyter": {"source_hidden": true}}
@pytest.fixture
def bucket_with_policy(versioned_bucket_with_lock_config, s3_client, lock_wait_time, request):
    bucket_name, _, second_object_key, _, _ = versioned_bucket_with_lock_config
    policy_doc = request.param

    # change the resource field to be the objects inside the bucket
    resource_template = policy_doc['Statement'][0]['Resource']
    policy_doc['Statement'][0]['Resource'] = resource_template.format(bucket_name=bucket_name)

    # set policy
    logging.info(f"put_bucket_policy: {bucket_name}, {policy_doc}")
    policy_put_result = s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy_doc))
    logging.info(f"put_bucket_policy result: {policy_put_result}")

    # wait for the bucket policy to be effective
    wait_time = lock_wait_time
    logging.info(f"Put bucket policy might take time to propagate. Wait more {wait_time} seconds")
    time.sleep(wait_time)

    return bucket_name, second_object_key
# -

# O exemplo abaixo tenta mudar a retention de um object, mas é impedida por conta da política:

# +
@pytest.mark.policy
@pytest.mark.parametrize('bucket_with_policy', [policy_template], indirect = True)
def test_policy_for_put_object_retention(bucket_with_policy, s3_client):
    bucket_name, object_key = bucket_with_policy

    # date in the futrure for the object retention
    retain_until_date = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=2)
    logging.info(f"retention date for {object_key} will be {retain_until_date}")

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        retention_update_response = s3_client.put_object_retention(
            Bucket=bucket_name,
            Key=object_key,
            Retention={ "RetainUntilDate": retain_until_date }
        )
    logging.info(f"exc_info.value.response {exc_info.value.response}")
    error_code = exc_info.value.response['Error']['Code']
    assert error_code == "AccessDeniedByBucketPolicy", f"Expected AccessDeniedByPolicy, got {error_code}"

run_example(__name__, "test_policy_for_put_object_retention", config=config,)
# -

# ## Referências
# - [Amazon S3 Object Lock Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) - Visão Geral sobre Object Lock no Amazon S3
# - [put_object_lock_configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object_lock_configuration.html) - Configurar Object Lock em um bucket
# - [get_object_lock_configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object_lock_configuration.html) - Recuperar configuração de Object Lock de um bucket
# - [Why can I delete objects even after I turned on Object Lock for my Amazon S3 bucket?](https://repost.aws/knowledge-center/s3-object-lock-delete) - Detalhamento de como deletar e gerenciar retenção e legal hold em objetos S3

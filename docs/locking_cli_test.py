# ---
# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # Uso de Object Locking via linha de comando
#
# A funcionalidade de object locking permite a configuração de travas contra a remoção de objetos
# por uma quantidades de dias determinada. Na MGC CLI, existem comandos para configurar estas travas
# tanto a nivel de bucket quanto de object, bem como comandos para consultar estas configurações.

# + tags=["parameters"]
config = "../params/aws-east-1.yaml"
docs_dir = "."
# -

# ## Exemplos

import pytest
import logging
import subprocess
import json
from shlex import split, quote
from s3_helpers import run_example

# ### Configurar uma trava padrão
#
# Para setar uma regra de retenção padrão para todos os novos objetos escritos em um bucket, utilize
# o comando `object-storage buckets object-lock set`, exemplos:

commands = [
    "{mgc_path} object-storage buckets object-lock set {bucket_name} --days {days}",
    "{mgc_path} os buckets object-lock set --dst {bucket_name} --days {days}",
]

@pytest.mark.parametrize("cmd_template", commands)
def test_set_bucket_default_lock(cmd_template, active_mgc_workspace, mgc_path, versioned_bucket_name_to_lock):
    days = "1"
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=versioned_bucket_name_to_lock, days=days))

    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "locking_cli", "test_set_bucket_default_lock", config=config, docs_dir=docs_dir)

# #### Bucket com trava para utilizar nos próximos exemplos

@pytest.fixture
def bucket_with_lock(versioned_bucket_name_to_lock, mgc_path, active_mgc_workspace):
    bucket_name = versioned_bucket_name_to_lock
    cmd = split(
        f"{mgc_path} object buckets object-lock set {bucket_name} --days 1"
    )
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"

    return bucket_name

# ### Consultar a configuração de locking em um bucket
#
# Na mgc cli, o comando para consultar os parametros do locking padrão de um bucket é o
# `buckets object-lock get`, exemplo:

commands = [
    "{mgc_path} object-storage buckets object-lock get {bucket_name}",
]

@pytest.mark.parametrize("cmd_template", commands)
def test_get_bucket_default_lock(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock):
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_with_lock))
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "locking_cli", "test_get_bucket_default_lock", config=config, docs_dir=docs_dir)


# ### Soft delete vs permanent delete
#
# Em um bucket com regra de object-lock, deletes simples ainda são permitidos,
# por conta do bucket ser versionado estes não destroem dados, apenas adicionam
# uma marca (delete marker). Já tentativas de deletar uma versão específica
# de um objeto (permanent delete) são barradas pela trava.

# #### Bucket versionado, com trava e um object para utilizar nos próximos exemplos

@pytest.fixture
def bucket_with_lock_and_object(active_mgc_workspace, mgc_path, bucket_with_lock):
    bucket_name = bucket_with_lock
    object_key = "key1"
    src = f"{docs_dir}/index.md"
    dst = f"{bucket_name}/{object_key}"

    # Upload a file to the versioned bucket
    cmd_str = f"{mgc_path} object-storage objects upload {src} {dst}"
    result = subprocess.run(split(cmd_str), capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"

    # List the object versions
    cmd_str = f"{mgc_path} object-storage objects versions {dst} --raw"
    result = subprocess.run(split(cmd_str), capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"

    # Retrieve the only version id in the list
    versions_output = json.loads(result.stdout)
    object_version = versions_output[0].get("VersionID")

    return bucket_name, object_key, object_version

# #### Soft delete
#
# Na mgc cli, um delete simples é feito com o comando `objects delete`, exemplo:

commands = [
    "{mgc_path} object-storage objects delete {bucket_name}/{object_key} --no-confirm",
]

@pytest.mark.parametrize("cmd_template", commands)
def test_simple_delete_object_on_locked_bucket(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock_and_object):
    bucket_name, object_key, _ = bucket_with_lock_and_object

    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd}: {result.stdout}")

run_example(__name__, "locking_cli", "test_simple_delete_object_on_locked_bucket", config=config, docs_dir=docs_dir)

# #### Permanent delete
#
# Na mgc cli, um delete permanente é feito com o mesmo comando `objects delete`, porém incluindo
# o parametro --obj-version para especificar qual versão de objeto deletar, exemplo:

commands = [
    "{mgc_path} object-storage objects delete {bucket_name}/{object_key} --no-confirm --obj-version {object_version}",
]

@pytest.mark.parametrize("cmd_template", commands)
def test_permanent_delete_object_on_locked_bucket(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock_and_object):
    bucket_name, object_key, object_version = bucket_with_lock_and_object

    cmd = split(f"{mgc_path} object-storage objects delete --no-confirm {bucket_name}/{object_key}  --obj-version {object_version}")
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key, object_version=object_version))
    result = subprocess.run(cmd, capture_output=True, text=True)
    # we do not assert the exit status of the command here because AWS may return a 200 with an AccessDenied xml inside and mgc cli will interpret it as success
    logging.info(f"Output from {cmd}: {result.stdout}")
    logging.info(f"Error from {cmd}: {result.stderr}")

    cmd_str = f"{mgc_path} object-storage objects versions {bucket_name}/{object_key} --raw "
    result = subprocess.run(split(cmd_str), capture_output=True, text=True)
    assert object_version in result.stdout, "Unexpected output: {result.stdout}"

run_example(__name__, "locking_cli", "test_permanent_delete_object_on_locked_bucket", config=config, docs_dir=docs_dir)

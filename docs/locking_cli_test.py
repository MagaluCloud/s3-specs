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

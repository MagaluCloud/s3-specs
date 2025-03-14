# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Bucket Policy via MGC CLI
#
# Restringir determinadas ações passíveis de serem executadas em um bucket, ou
# compartilhar acessos de leitura e escrita com outros usuários (Principals),
# são exemplos de possibilidades que a funcionalidade de Bucket Policy (políticas
# do conteiner) permitem.
#
# A configuração, remoção e atualização de políticas em um bucket, por documentos
# de policy (arquivos JSON) pode ser feita via MGC CLI. Este é o assunto desta
# especificação.
#

# + tags=["parameters"]
config = "../params.example.yaml"
config = "../params.yaml"
config = "../params/br-ne1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
import subprocess
from shlex import split
from s3_helpers import (
    run_example,
)
pytestmark = [pytest.mark.basic, pytest.mark.cli]
# -
# ## Como fazer

# ### Listar os comandos disponíveis
#
# O manual de uso com os comandos disponíveis na CLI relacionados a Bucket Policy
# pode ser consultado via terminal usando o comando abaixo:

commands = [
    "{mgc_path} object-storage buckets policy --help",
]

# + {"jupyter": {"source_hidden": true}}
@pytest.mark.parametrize("cmd_template", commands)
def test_cli_help_policy(cmd_template, mgc_path):
    cmd = split(cmd_template.format(mgc_path=mgc_path))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.debug(f"Output from {cmd_template}: {result.stdout}")
    assert all(snippet in result.stdout for snippet in ["delete", "get", "set"])

run_example(__name__, "test_cli_help_policy", config=config)
# -

# ### Documento de política válido
#
# Para fins de exemplificar o uso dos comandos, usaremos um documento JSON de política
# válido bem simples, que habilita o download de todos os objetos (`*`) em um bucket
# (`Resource`) para qualquer (`*`) usuário (`Principal`).

# +
@pytest.fixture
def valid_simple_policy():
    return """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "<bucket_name>/*"
            }
        ]
    }"""
# -

# > **Nota:** Substitua `<bucket_name>` pelo nome do seu bucket.

# ### Atribuir uma política a um bucket
#
# Para definir uma política de bucket usando a MGC CLI, utilize o comando:

set_policy_commands = [
    "{mgc_path} object-storage buckets policy set --dst {bucket_name} --policy '{policy_json_content}'",
]

# + {"jupyter": {"source_hidden": true}}
@pytest.mark.parametrize("cmd_template", set_policy_commands)
def test_cli_set_policy(cmd_template, active_mgc_workspace, mgc_path, valid_simple_policy, existing_bucket_name):
    bucket_name = existing_bucket_name
    policy = valid_simple_policy.replace("<bucket_name>", bucket_name)
    logging.info(policy)
    cmd = split(cmd_template.format(
        mgc_path=mgc_path,
        bucket_name=bucket_name,
        policy_json_content=policy
    ))
    logging.info(f"{cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_cli_set_policy", config=config)
# -

# ### Remover uma política de um bucket
#
# Para deletar uma política de bucket usando a MGC CLI, utilize o comando:

delete_policy_commands = [
    "{mgc_path} object-storage buckets policy delete --dst {bucket_name}",
]

# + {"jupyter": {"source_hidden": true}}
from utils.policy import fixture_bucket_with_policy

@pytest.mark.parametrize(
    "cmd_template",
    delete_policy_commands,
)
def test_cli_delete_policy(cmd_template, fixture_bucket_with_policy, active_mgc_workspace, mgc_path):
    bucket_name, _policy_doc, _s3_clients, _object_prefix, _content = fixture_bucket_with_policy
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name))
    logging.info(f"{cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_cli_delete_policy", config=config)
# -

# ## Documentos relacionados
#
# Uma breve lista de outras referências sobre Bucket Policy
#
# - Docs Magalu Cloud: [Docs > Armazenamento > Object Storage > Como Fazer > Permissões > Bucket Policy](https://docs.magalu.cloud/docs/storage/object-storage/how-to/permissions/policies)
# - Docs Magalu Cloud: [Docs > Armazenamento > Object Storage > Controle de Acesso de Dados > Bucket Policy](https://docs.magalu.cloud/docs/storage/object-storage/access-control/bucket_policy_overview/)
# - s3-specs: [python/boto3 policies spec 1](https://magalucloud.github.io/s3-specs/runs/profiles_policies_test_br-ne1.html)
# - s3-specs: [python/boto3 policies spec 2](https://magalucloud.github.io/s3-specs/runs/policies_test_test_br-ne1.html)
# - s3-tester (specs legadas): [legacy s3-tester spec id:091](https://github.com/MagaluCloud/s3-tester/blob/main/spec/091_bucket-policy_spec.sh)
# - AWS S3: [Bucket Policies for Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-policies.html)
# - AWS S3: [Bucket policies](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-bucket-policies.html)


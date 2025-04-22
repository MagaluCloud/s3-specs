# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Listagem de Objeto utilizando a CLI MGC
# 
# O comando para a listagem de objetos em um bucket via mgc cli é o `object-storage objects list`
# junto com o nome do bucket é possivel incluir um prefixo, considerando que é comum ferramentas
# de sync de diretórios usarem nomes de objetos em um formato de "path"
# como nos exemplos abaixo:

commands = [
    "{mgc_path} object-storage objects list '{bucket_name}'",
    "{mgc_path} os objects list '{bucket_name}/{object_prefix}'",
    "aws --profile {profile_name} s3api list-objects-v2 --bucket '{bucket_name}'",
    "aws --profile {profile_name} s3api list-objects-v2 --bucket '{bucket_name}' --prefix '{object_prefix}'"
]

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
import subprocess
from shlex import split 
from s3_specs.docs.s3_helpers import (
    run_example,
)
pytestmark = [pytest.mark.basic, pytest.mark.cli]

# -

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# +

test_buckets = [
    {
        "object_prefix": "",
        "object_key_list": ["simple-object-key.txt"],
    },
    {
        "object_prefix": "simple-prefix/",
        "object_key_list": [ "sufix-1.txt", "sufix-2.txt", ]
    },
    {
        "object_prefix": "prefix/with/multiple/slashes/",
        "object_key_list": [
            "acentuação e emojis 🎉 🥳 😘-1.txt",
            "acentuação e emojis 😘 🎉 🥳-2.txt",
        ]
    },
    {
        "object_prefix": "prefix/with/multiple/slashes/and/someparam=Name with space/",
        "object_key_list": [
            "file1.txt",
            "file 2 with space.txt",
        ]
    }
]

special_characteres = ["*", "+", "-", "_", ".", "(parentesis)", "&", "$", "@", "=", ";", ":", "   ", ",", "?"]

for special_charactere in special_characteres:
    test_buckets.append({
        "object_prefix": f"prefix/with/multiple/slashes/and/character {special_charactere} in key/",
        "object_key_list": ["file.txt"]
    })

logging.info(f"Test cases: {test_buckets}")

test_cases = [
    (command, test_bucket)
    for command in commands
    for test_bucket in test_buckets
]

@pytest.mark.parametrize(
    "cmd_template, bucket_with_many_objects_session",
    test_cases,
    indirect=["bucket_with_many_objects_session"]
)
def test_list_objects_cli(cmd_template, bucket_with_many_objects_session, profile_name, active_mgc_workspace, mgc_path):
    bucket_name, object_prefix, _content, _ = bucket_with_many_objects_session
    cmd = split(cmd_template.format(mgc_path=mgc_path, profile_name=profile_name, bucket_name=bucket_name, object_prefix=object_prefix))

    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_list_objects_cli", config=config)
# -

# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Download de Objeto utilizando a CLI MGC
# 
# O comando para o download de um objeto via mgc cli é o `object-storage objects download`
# como exemplificado nos exemplos abaixo:

commands = [
    "{mgc_path} object-storage objects download '{bucket_name}/{object_key}' '{local_path}'",
    "{mgc_path} os objects download '{bucket_name}/{object_key}' '{local_path}'",
]

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
import subprocess
from shlex import split
from s3_specs.docs.s3_helpers import (
    run_example,
)
pytestmark = [pytest.mark.basic, pytest.mark.cli, pytest.mark.homologacao]
# -

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# +
object_keys = [
    "test-object.txt",
    "test/object/sub/folder/😘 Arquivo com espaço e acentuação 🍕.txt",
]
test_cases = [
    (command, {'object_key': object_key}) 
    for command in commands
    for object_key in object_keys
]

@pytest.mark.parametrize(
    "cmd_template, bucket_with_one_object",
    test_cases,
    indirect=["bucket_with_one_object"]
)
def test_download_object_cli(cmd_template, bucket_with_one_object, active_mgc_workspace, mgc_path):
    local_path = "/tmp/downloaded test file.txt"
    bucket_name, object_key, content = bucket_with_one_object
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key, local_path=local_path))

    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_download_object_cli", config=config)
# -

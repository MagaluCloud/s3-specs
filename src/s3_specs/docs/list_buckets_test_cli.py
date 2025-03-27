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

# # List buckets
#
# Lista os buckets de um perfil[<sup>1</sup>](../glossary#profile)


# + {"tags": ["parameters"]}
config = "../params/br-ne1.yaml"

# + {"jupyter": {"source_hidden": true}}
import pytest
import os
import logging
import subprocess
from shlex import split
from .s3_helpers import run_example

pytestmark = [pytest.mark.basic, pytest.mark.cli]
config = os.getenv("CONFIG", config)
# -

# ## Exemplos
#
# -
# ### Rclone e AWS CLI
#
# O comando para listar buckets no rclone é o `lsd`.
# Os comandos para listar buckets na awscli são `s3 ls` e `s3api list-buckets`.
#
# **Exemplos:**

commands = [
    "rclone lsd {profile_name}:",
    "aws s3 ls --profile {profile_name}",
    "aws s3api list-buckets --profile {profile_name}",
]

# +
@pytest.mark.parametrize("cmd_template", commands)
def test_list_buckets_cli(cmd_template, profile_name):
    cmd = split(cmd_template.format(profile_name=profile_name))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_cli_list_buckets", config=config)
# -

# ## Referências
#
# - [rclone lsd](https://rclone.org/commands/rclone_lsd/)
# - [aws cli ls](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html)
# - [aws cli list-buckets](https://docs.aws.amazon.com/cli/latest/reference/s3api/list-buckets.html)

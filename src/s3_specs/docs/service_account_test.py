# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
import subprocess
import os
from shlex import split
from itertools import product 
from s3_specs.docs.tools.utils import execute_subprocess
from s3_specs.docs.tools.service_account import profile_name_sa, active_mgc_workspace_sa, get_sa_infos, bucket_with_one_object_and_bucket_policy
from pathlib import Path # Para lidar com caminhos de arquivo de forma mais robusta

pytestmark = [pytest.mark.service_account, pytest.mark.cli]

commands = [
    pytest.param(
        "aws --profile {profile_name} s3api list-buckets",
        "AccessDenied",
        "list",
        marks=pytest.mark.aws,
        id="aws-list-buckets"
    ),
    pytest.param(
        "rclone lsd {profile_name}:",
        "AccessDenied",
        "list",
        marks=pytest.mark.rclone,
        id="rclone-list-buckets"
    ),
    pytest.param(
        "mgc os buckets list",
        "AccessDenied",
        "list",
        marks=pytest.mark.mgc,
        id="mgc-list-buckets"
    ),
    pytest.param(
        "aws --profile {profile_name} s3api create-bucket --bucket {bucket_name}",
        "AccessDenied",
        "create",
        marks=pytest.mark.aws,
        id="aws-create-bucket"
    ),
    pytest.param(
        "rclone mkdir {profile_name}:{bucket_name}",
        "AccessDenied",
        "create",
        marks=pytest.mark.rclone,
        id="rclone-create-bucket"
    ),
    pytest.param(
        "mgc os buckets create --bucket {bucket_name} --raw",
        "AccessDenied",
        "create",
        marks=pytest.mark.mgc,
        id="mgc-create-bucket"
    )
]

@pytest.mark.parametrize("cmd_template, expected, action", commands)
def test_service_account_deny_operation(active_mgc_workspace_sa, profile_name_sa, bucket_name, cmd_template, expected, action):
    try:
        if action == "list":
            formatted_cmd = cmd_template.format(profile_name=f"{profile_name_sa}")
        elif action == "create":
            formatted_cmd = cmd_template.format(profile_name=f"{profile_name_sa}", bucket_name=bucket_name)
            
    except KeyError: 
        formatted_cmd = cmd_template

    logging.info(f"Command to run: {formatted_cmd}")
    logging.info(f"Expected result keyword: {expected}")

    process = subprocess.run(formatted_cmd, shell=True, capture_output=True, text=True)
    output = process.stdout + process.stderr

    logging.info(f"Output: {output}")

    assert expected in output, f"Expected '{expected}' not found in output for action '{action}':\n{output}"




commands = [
    pytest.param(
        "aws --profile {profile_name} s3api get-object --bucket {bucket_name} --key {object_key} {local_file}",
        "get-object",
        marks=pytest.mark.aws,
        id="aws-get-object"
    ),
    # pytest.param(
    #     "rclone lsd {profile_name}:",
    #     "AccessDenied",
    #     "get-object",
    #     marks=pytest.mark.rclone,
    #     id="rclone-get-object"
    # ),
    # pytest.param(
    #     "mgc os buckets get-object",
    #     "AccessDenied",
    #     "get-object",
    #     marks=pytest.mark.mgc,
    #     id="mgc-get-object"
    # )
]

@pytest.mark.parametrize(
    "cmd_template, action", commands  
)
def test_service_account_allow_operation(s3_client, active_mgc_workspace_sa, profile_name_sa, get_sa_infos, bucket_with_one_object_and_bucket_policy, cmd_template, action, tmp_path):
    bucket_name = None
    local_file = None

    try:
        principal_str = f"{get_sa_infos['sa_tenant_id']}:sa/{get_sa_infos['sa_key_email']}"
        logging.info(f"Aplicando política ALLOW para Action: {action}, Principal: {principal_str}")
        bucket_name, object_key = bucket_with_one_object_and_bucket_policy(
            actions=action,
            principal=principal_str,
            effect="Allow"
        )
        logging.info(f"Política aplicada ao bucket '{bucket_name}' para o objeto '{object_key}'")

        local_file = tmp_path / "downloaded_object.txt"

        try:
            formatted_cmd = cmd_template.format(
                profile_name=profile_name_sa,
                bucket_name=bucket_name,
                object_key=object_key,
                local_file=str(local_file)
            )
        except KeyError as e:
            formatted_cmd = cmd_template.format(
                profile_name=profile_name_sa,
                bucket_name=bucket_name,
                object_key=object_key
            )


        logging.info(f"Executando comando: {formatted_cmd}")
        result = subprocess.run(formatted_cmd, shell=True, capture_output=True, text=True, check=False)

        logging.info(f"Comando finalizado com código de saída: {result.returncode}")
        if result.stdout:
            logging.info(f"Output:\n{result.stdout}")
            assert "ETag" in result.stdout

        assert result.returncode == 0, f"Comando falhou inesperadamente com código {result.returncode}. Stderr: {result.stderr}"
        
        assert local_file.exists(), f"O arquivo local {local_file} não foi criado pelo get-object."        

        # Verifica se o usuario pode fazer put object
        cmd = ["aws", "--profile", profile_name_sa, "s3api", "put-object","--bucket", bucket_name, "--key", object_key, "--body", local_file]
        expected = "AccessDenied"

        process = subprocess.run(cmd, capture_output=True, text=True)
        
        output = process.stdout + process.stderr

        logging.info(f"Output: {output}")

        assert expected in output, f"Expected '{expected}' not found in output for action '{action}':\n{output}"

    finally:
        # --- Teardown ---
        if bucket_name:
            try:
                logging.info(f"Removendo política do bucket {bucket_name}")
                s3_client.delete_bucket_policy(Bucket=bucket_name)
            except Exception as e:
                logging.info(f"Erro ao tentar remover política do bucket {bucket_name}: {e}", exc_info=True)

        if local_file and local_file.exists():
             try:
                 logging.info(f"Removendo arquivo local {local_file}")
                 local_file.unlink()
             except Exception as e:
                 logging.error(f"Erro ao tentar remover arquivo local {local_file}: {e}", exc_info=True)    



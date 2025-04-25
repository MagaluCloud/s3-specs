
import pytest
import logging
from s3_specs.docs.tools.utils import execute_subprocess
import subprocess
from s3_specs.docs.tools.permission import fixture_public_bucket, fixture_private_bucket, profile_name_second, active_mgc_workspace_second # , fixture_private_bucket_no_access # Exemplo
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import get_tenants

pytestmark = pytest.mark.acl
config = "../params/br-se1.yaml"
logging.basicConfig(level=logging.INFO)

list_commands = [
    pytest.param(
        {"command": "mgc object-storage objects list {bucket_name} --raw",
        'expected': "403"},
        marks=pytest.mark.mgc,
        id="mgc-list"
    ),
    pytest.param(
        {"command":"aws --profile {profile_name_second} s3 ls s3://{bucket_name}",
        'expected': "AccessDenied"},
        marks=pytest.mark.aws,
        id="aws-list"
    ),
    pytest.param(
        {"command":"rclone ls {profile_name_second}:{bucket_name}",
        'expected': "403"},
        marks=pytest.mark.rclone,
        id="rclone-list"
    )
]

download_commands = [
    pytest.param(
        {
            "command": "mgc object-storage objects download {bucket_name}/{obj_key} {local_path}",
            "expected": "403",
        },
        marks=pytest.mark.mgc,
        id="mgc-download-fail"
    ),
    pytest.param(
        {
            "command": "aws --profile {profile_name_second} s3 cp s3://{bucket_name}/{obj_key} {local_path}",
            "expected": "403",
        },
        marks=pytest.mark.aws,
        id="aws-download-fail"
    ),
    pytest.param(
        {
            "command": "rclone copyto {profile_name_second}:{bucket_name}/{obj_key} {local_path} -vv",
            "expected": "There was nothing to transfer",
        },
        marks=pytest.mark.rclone,
        id="rclone-download-fail"
    )
]

@pytest.mark.parametrize("cmd_template", list_commands)
def test_list_objects(active_mgc_workspace_second, fixture_public_bucket, cmd_template, profile_name_second):
    """Verifica se é possível listar objetos em um bucket público usando diferentes CLIs."""
    bucket_name, obj_key = fixture_public_bucket
    cmd = cmd_template["command"].format(
        bucket_name=bucket_name,
        profile_name_second=profile_name_second
    )
    logging.info(f"Executing list command: {cmd}")
    result = execute_subprocess(cmd)
    assert result.returncode == 0, (
        f"List command failed with exit code {result.returncode}\n"
        f"Command: {cmd}\n"
        f"Error: {result.stderr}"
    )
    assert obj_key in result.stdout.strip(), (
        f"Expected object key '{obj_key}' not found in list command output:\n"
        f"Command: {cmd}\n"
        f"Output:\n{result.stdout}"
    )

## expected failure
@pytest.mark.parametrize("cmd_template", download_commands)
def test_download_object_expected_failure(
    active_mgc_workspace_second,
    fixture_public_bucket,
    cmd_template,
    profile_name_second,
    tmp_path
):
    """
    Verifica se a tentativa de download de um objeto FALHA como esperado,
    se o erro esperado está no stderr, e se o arquivo local não foi criado.
    """
    bucket_name, obj_key = fixture_public_bucket

    local_path = tmp_path / f"failed_download_attempt_{obj_key.replace('/', '_')}.tmp"
    logging.info(f"Attempting download to non-existent target: {local_path}")

    command_string = cmd_template["command"]
    expected_output_in_stderr = cmd_template.get("expected")
    if expected_output_in_stderr is None:
        pytest.fail(f"Test parameterization missing 'expected' key for ID: {cmd_template.get('id', 'UNKNOWN')}")

    cmd = command_string.format(
        bucket_name=bucket_name,
        obj_key=obj_key,
        profile_name_second=profile_name_second,
        local_path=str(local_path)
    )

    logging.info(f"Executing download command (EXPECTED TO FAIL): {cmd}")

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)

    assert expected_output_in_stderr in result.stderr, (
        f"Erro esperado '{expected_output_in_stderr}' não encontrado no stderr:\n{result.stderr}"
    )

    assert not local_path.exists(), f"O arquivo {local_path} foi criado, mas o download deveria ter falhado."

## expected failure
@pytest.mark.parametrize("cmd_template", list_commands)
def test_list_objects_expected_failure(
    active_mgc_workspace_second,
    fixture_private_bucket,
    cmd_template,
    profile_name_second,
    tmp_path
):
    """
    Verifica se a tentativa de download de um objeto FALHA como esperado,
    se o erro esperado está no stderr, e se o arquivo local não foi criado.
    """
    bucket_name, obj_key = fixture_private_bucket

    local_path = tmp_path / f"failed_download_attempt_{obj_key.replace('/', '_')}.tmp"
    logging.info(f"Attempting download to non-existent target: {local_path}")

    command_string = cmd_template["command"]
    expected_output_in_stderr = cmd_template.get("expected")
    if expected_output_in_stderr is None:
        pytest.fail(f"Test parameterization missing 'expected' key for ID: {cmd_template.get('id', 'UNKNOWN')}")

    cmd = command_string.format(
        bucket_name=bucket_name,
        obj_key=obj_key,
        profile_name_second=profile_name_second,
        local_path=str(local_path)
    )

    logging.info(f"Executing list bucket command (EXPECTED TO FAIL): {cmd}")

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)

    # Verifica que o erro esperado está no stderr
    assert expected_output_in_stderr in result.stderr, (
        f"Erro esperado '{expected_output_in_stderr}' não encontrado no stderr:\n{result.stderr}"
    )

    # Verifica que o arquivo não foi criado
    assert not local_path.exists(), f"O arquivo {local_path} foi criado, mas o download deveria ter falhado."


acls_to_test = [
    pytest.param({"acl_type": "canned", "acl_name": "public-read"}, id="public-read"),
    pytest.param({"acl_type": "canned", "acl_name": "private"}, id="private"),
    pytest.param({"acl_type": "grant", "permission": "READ", "mgc_grant_flag": None, "aws_grant_flag": "read"}, id="grant-read"),
    pytest.param({"acl_type": "grant", "permission": "READ_ACP", "mgc_grant_flag": None, "aws_grant_flag": "read-acp"}, id="grant-read-acp"),
    pytest.param({"acl_type": "grant", "permission": "WRITE_ACP", "mgc_grant_flag": None, "aws_grant_flag": "write-acp"}, id="grant-write-acp"),
    pytest.param({"acl_type": "grant", "permission": "WRITE", "mgc_grant_flag": None, "aws_grant_flag": "write"}, id="grant-write"),
    pytest.param({"acl_type": "grant", "permission": "FULL_CONTROL", "mgc_grant_flag": None, "aws_grant_flag": "full-control"}, id="grant-full-control"),
]

# Define as estruturas de comando para cada CLI
cli_commands = [
    pytest.param({
        "cli": "mgc",
        "canned_cmd": "mgc workspace set {profile_name} && mgc object-storage objects acl set --dst={bucket_name}/{obj_key} --{acl_name}",
        "grant_cmd": "mgc workspace set {profile_name} &&  mgc object-storage objects acl set --dst={bucket_name}/{obj_key} --grant-{mgc_grant_flag} id={grantee_id}"
    }, id="mgc-put-acl"),
    pytest.param({
        "cli": "aws",
        "canned_cmd": "aws s3api --profile {profile_name} put-object-acl --bucket {bucket_name} --key {obj_key} --acl {acl_name}",
        "grant_cmd": "aws s3api --profile {profile_name} put-object-acl --bucket {bucket_name} --key {obj_key} --grant-{aws_grant_flag} id={grantee_id}"
    }, id="aws-put-acl"),
]


@pytest.mark.parametrize("acl_info", acls_to_test)
@pytest.mark.parametrize("cli_info", cli_commands)
def test_put_object_acl(
    active_mgc_workspace_second,
    multiple_s3_clients,
    fixture_private_bucket,
    profile_name,
    cli_info,
    acl_info
):
    """Testa a aplicação de diferentes ACLs a um objeto usando MGC e AWS CLI."""
    bucket_name, obj_key = fixture_private_bucket
    cli_name = cli_info["cli"]
    acl_type = acl_info["acl_type"]
    tenants = get_tenants(multiple_s3_clients)
    s3_other = tenants[1]

    cmd = None
    format_params = {
        "bucket_name": bucket_name,
        "obj_key": obj_key,
        "profile_name": profile_name, # Comando é executado pelo dono
        "grantee_id": s3_other
    }

    if acl_type == "canned":
        # Configura para ACL pré-definida
        acl_name = acl_info["acl_name"]
        format_params["acl_name"] = acl_name
        # Usa replace para garantir que o nome da acl (ex: public-read) vire a flag correta
        # (ex: --public-read para MGC, --acl public-read para AWS)
        cmd_template = cli_info["canned_cmd"]
        cmd = cmd_template.format(**format_params)

    elif acl_type == "grant":
        permission = acl_info["permission"]
        grant_flag = None

        if cli_name == "mgc":
            grant_flag = acl_info.get("mgc_grant_flag")
            format_params["mgc_grant_flag"] = grant_flag
        elif cli_name == "aws":
            grant_flag = acl_info.get("aws_grant_flag")
            format_params["aws_grant_flag"] = grant_flag.replace('_', '-') if grant_flag else None

        if grant_flag is None:
            pytest.skip(f"ACL grant '{permission}' não configurada/suportada para CLI '{cli_name}' neste teste.")

        cmd_template = cli_info["grant_cmd"]
        cmd = cmd_template.format(**format_params)

    if cmd is None:
         pytest.fail(f"Falha ao gerar o comando para {cli_name} com ACL {acl_info}")

    logging.info(f"Executando: {cmd}")
    result = execute_subprocess(cmd)

    assert result.returncode == 0, (
        f"Falha ao executar put-object-acl.\n"
        f"Comando: {cmd}\n"
        f"Exit Code: {result.returncode}\n"
        f"Stderr: {result.stderr}\n"
        f"Stdout: {result.stdout}"
    )

acls_to_test = [
    pytest.param({"acl_type": "canned", "acl_name": "public-read", "expected_read_ok": True}, id="acl=public-read"),
    pytest.param({"acl_type": "canned", "acl_name": "private", "expected_read_ok": False}, id="acl=private"),
    pytest.param({"acl_type": "grant", "permission": "READ", "aws_permission": "READ", "expected_read_ok": True}, id="acl=grant-read"),
    pytest.param({"acl_type": "grant", "permission": "READ_ACP", "aws_permission": "READ_ACP", "expected_read_ok": False}, id="acl=grant-read-acp"),
    pytest.param({"acl_type": "grant", "permission": "WRITE_ACP", "aws_permission": "WRITE_ACP", "expected_read_ok": False}, id="acl=grant-write-acp"),
    pytest.param({"acl_type": "grant", "permission": "FULL_CONTROL", "aws_permission": "FULL_CONTROL", "expected_read_ok": True}, id="acl=grant-full-control"),
]

cli_validate_templates = {
    "mgc": {
        "download_cmd": "mgc object-storage objects download {bucket_name}/{obj_key} {local_path}",
    },
    "aws": {
        "download_cmd": "aws s3 cp --profile {profile_name} s3://{bucket_name}/{obj_key} {local_path}",
    },
    "rclone": {
        "download_cmd": "rclone copyto {profile_name}:{bucket_name}/{obj_key} {local_path}",
    }
}

@pytest.mark.parametrize("acl_info", acls_to_test) # Itera sobre as ACLs (que contêm a expectativa)
@pytest.mark.parametrize("validate_cli_name, validate_cmds", cli_validate_templates.items()) # Itera sobre as CLIs de validação
def test_set_acl_boto3_validate_cli_read(
    active_mgc_workspace_second,
    multiple_s3_clients,
    fixture_private_bucket,
    profile_name_second,
    acl_info,
    validate_cli_name,
    validate_cmds,
    tmp_path
):
    """
    Define ACL via Boto3, valida acesso de LEITURA via CLI (secundário),
    usando a expectativa definida junto com a ACL.
    """
    tenants = get_tenants(multiple_s3_clients)

    s3_client_primary = multiple_s3_clients[0]
    owner_id = tenants[0]
    tenant_id_second = tenants[1]
    bucket_name, obj_key = fixture_private_bucket
    try: # Obtém Owner ID necessário para grants Boto3
        owner_info = s3_client_primary.get_object_acl(Bucket=bucket_name, Key=obj_key)['Owner']
    except ClientError as e:
        pytest.fail(f"Falha ao obter Owner ID inicial: {e}")

    expected_read_ok = acl_info["expected_read_ok"]
    acl_id_str = acl_info.get('acl_name') or acl_info.get('permission', 'N/A')
    
    try:
        if acl_info["acl_type"] == "canned":
            s3_client_primary.put_object_acl(Bucket=bucket_name, Key=obj_key, ACL=acl_info["acl_name"])
        elif acl_info["acl_type"] == "grant":
            permission = acl_info["aws_permission"]
            grant_policy = {
                'Owner': owner_info,
                'Grants': [
                    {'Grantee': {'Type': 'CanonicalUser', 'ID': tenant_id_second}, 'Permission': permission},
                    {'Grantee': {'Type': 'CanonicalUser', 'ID': owner_id}, 'Permission': 'FULL_CONTROL'}
                ]
            }
            s3_client_primary.put_object_acl(Bucket=bucket_name, Key=obj_key, AccessControlPolicy=grant_policy)
    except ClientError as e:
        pytest.fail(f"Boto3 Error setting ACL '{acl_id_str}': {e}")
    except Exception as e:
        pytest.fail(f"Unexpected Error setting ACL '{acl_id_str}' via Boto3: {e}")

    validation_local_path = tmp_path / f"validate_{validate_cli_name}_{acl_id_str}.tmp"

    validate_template = validate_cmds.get("download_cmd")
    if not validate_template:
         pytest.skip(f"Validation command (download) not defined for CLI '{validate_cli_name}'")

    format_params_validate = {
        "bucket_name": bucket_name,
        "obj_key": obj_key,
        "profile_name": profile_name_second,
        "local_path": str(validation_local_path)
    }

    cmd_validate = validate_template.format(**format_params_validate)

    expect_cmd_fail = not expected_read_ok


    try:
        result_validate = execute_subprocess(cmd_validate, expected_failure=expect_cmd_fail)
    except Exception as e:
        pytest.fail(f"execute_subprocess failed or expectation not met: {e}\nCommand: {cmd_validate}\nExpected failure: {expect_cmd_fail}")

    if expected_read_ok:
        assert validation_local_path.is_file(), (
            f"VALIDATION FAILED: File not found at {validation_local_path} but SUCCESS was expected.\n"
            f"ACL Set (Boto3): {acl_id_str}\n"
            f"Validation Command ({validate_cli_name}): {cmd_validate}"
        )
        logging.info(f"ETAPA 2: Validation SUCCEEDED (command executed OK and file found).")
    else:
        assert not validation_local_path.exists(), (
            f"VALIDATION FAILED: File found at {validation_local_path} but FAILURE was expected.\n"
            f"ACL Set (Boto3): {acl_id_str}\n"
            f"Validation Command ({validate_cli_name}): {cmd_validate}"
        )

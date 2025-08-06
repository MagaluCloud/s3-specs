# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Bucket Policy (política de bucket)
#
# Buckets de armazenamento por padrão são acessíveis apenas pela dona da conta que criou este
# recurso. Também por padrão, esta dona possui a permissão de executar **qualquer** operação
# neste bucket e nos seus objetos. Configurar uma política de bucket é uma maneira de
# modificar estas permissões padrões, seja para **restringir** de forma granular o número de operações
# que podem ser executadas em um bucket ou objeto, e por quais contas (_"Principals"_, no sentido
# de beneficiários, outorgados), seja para **conceder** mais acessos a determinados recursos, e
# para quais contas.

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import os
import json
import pytest
import logging
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import (
    run_example,
    change_policies_json,
    create_policy_json,
)
import time

config = os.getenv("CONFIG", config)
pytestmark = [pytest.mark.policy, pytest.mark.skip_if_dev, pytest.mark.homologacao]
# -

# Políticas de bucket são descritas por meio de arquivos no formato JSON, que seguem uma gramática
# específica, devem conter uma lista de regras `Statement` onde cada ítem desta lista descreve um
# `Effect` (`Allow` ou `Deny`), um campo `Principal` para descrever a(s) beneficiária(s) da regra
# um campo `Action` com as operações que esta regra permite ou nega e um campo `Resource`, que
# define a qual recurso esta regra se aplicará. As regras desta sintaxe podem ser consultadas no
# documento [Estrutura de uma Bucket Policy](https://docs.magalu.cloud/docs/storage/object-storage/access-control/bucket_policy_overview#estrutura-de-uma-bucket-policy)
# Abaixo um modelo de documento de política sem os campos preenchidos:

policy_dict_template = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "", "Principal": "", "Action": "", "Resource": ""}],
}

policy_allow_specific_ip_only = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": ["BUCKET_NAME", "BUCKET_NAME/*"],
            "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.42"}},
        }
    ],
}

policy_deny_specific_ip_only = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": ["BUCKET_NAME", "BUCKET_NAME/*"],
        },
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": ["BUCKET_NAME", "BUCKET_NAME/*"],
            "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.42"}},
        },
    ],
}

test_cases_policy_ip = [
    # Cenário 1: [Allow with IP condition] - IP de teste não é 203.0.113.42 → não da match → resultado esperado: DENY
    ({"number_clients": 2}, policy_allow_specific_ip_only, "get_object", "deny"),
    # Cenário 2: [Allow sem condição + Deny com IP condition] - Allow bate, Deny não da match → resultado esperado: ALLOW
    ({"number_clients": 2}, policy_deny_specific_ip_only, "get_object", "allow"),
]


@pytest.mark.parametrize(
    "multiple_s3_clients, policy_template, boto3_action, expected_result",
    test_cases_policy_ip,
    indirect=["multiple_s3_clients"],
    ids=[
        "ip_allow_only_specific_ip_not_matched",
        "ip_deny_only_specific_ip_not_matched",
    ],
)
def test_policy_ip_conditions(
    multiple_s3_clients,
    bucket_with_one_object,
    policy_template,
    boto3_action,
    expected_result,
):
    s3_client = multiple_s3_clients[0]
    bucket_name, object_key, _ = bucket_with_one_object

    policy_doc = create_policy_json(bucket_name, policy_template)
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_doc)
    time.sleep(120)

    kwargs = {"Bucket": bucket_name, "Key": object_key}
    method = getattr(s3_client, boto3_action)

    if expected_result == "allow":
        # Esperamos que a operação seja bem-sucedida
        response = method(**kwargs)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    else:
        # Esperamos que a operação seja negada com erro AccessDeniedByBucketPolicy
        try:
            response = method(**kwargs)
            pytest.fail("Expected AccessDeniedByBucketPolicy exception not raised")
        except ClientError as e:
            assert e.response["Error"]["Code"] == "AccessDeniedByBucketPolicy"

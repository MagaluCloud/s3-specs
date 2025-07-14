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
import pytest
import logging
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from s3_specs.docs.s3_helpers import(
    run_example,
    change_policies_json,
)
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
    "Statement": [
        {
            "Effect": "",
            "Principal": "",
            "Action": "",
            "Resource": ""
        }
    ]
}

# + {"jupyter": {"source_hidden": true}}
# Exemplos de documentos inválidos:
malformed_policy_json ='''{
    "Version": "2012-10-18",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "meu-bucket/*"
        }
    ]
}'''

misspeled_policy = """{
    "Version": "2012-10-18",
    "Statement": [
        {
            ¨¨ dsa
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "vosso-bucket/*"
        }
    ]
}
"""
wrong_version_policy = """{
    "Version": "2012-10-18",
    "Statement": 
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "teu-bucket/*"
        }
    ]
}
"""

cases = [
    *[(case, "MalformedJSON") for case in ['', 'jason',"''", misspeled_policy, wrong_version_policy]],
    *[(case, "MalformedPolicy") for case in ['{}','""', malformed_policy_json]],
 ]   
# -

# ## Atribuindo uma política a um bucket usando Python
#
# O método para atribuir uma _bucket policy_ na biblioteca boto3 é o
# [put_bucket_policy](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_policy.html),
# se o documento de política for válido o retorno trará um HTTPStatusCode `204` enquanto
# se o documento for inválido por algum motivo uma _exception_ do tipo `ClientError` será levantada,
# como mostram os testes abaixo:

@pytest.mark.parametrize('input, expected_error', cases)
def test_put_invalid_bucket_policy(s3_client, existing_bucket_name, input, expected_error):
    try:
        s3_client.put_bucket_policy(Bucket=existing_bucket_name, Policy=input)
        pytest.fail("Expected exception not raised")
    except ClientError as e:
        # Assert the error code matches the expected one 
        assert e.response['Error']['Code'] == expected_error
run_example(__name__, "test_put_invalid_bucket_policy", config=config)

test_cases_actions = [
    "s3:PutObject",
    "s3:GetObject",
    "s3:DeleteObject",
    "s3:GetBucketObjectLockConfiguration",
    "s3:GetObjectRetention",
    "s3:PutBucketObjectLockConfiguration",
    "s3:PutObjectRetention",
]
test_cases_parameters = [ 
    {"policy_dict": policy_dict_template, "actions": action, "effect": "Deny"}
    for action in test_cases_actions
]
@pytest.mark.parametrize('policies_args', test_cases_parameters, ids=test_cases_actions)
def test_setup_policies(s3_client, existing_bucket_name, policies_args):
    bucket_name = existing_bucket_name

    # fill up the policy template with the parametrized Action and Effect
    policy_doc = change_policies_json(existing_bucket_name, policies_args, "*")

    logging.info(f"put_bucket_policy: {policy_doc}")
    response = s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_doc) 
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
run_example(__name__, "test_setup_policies", config=config)

# ## Negar operações específicas em objetos
#
# Regras com o `Effect` `Deny` impedem que determinadas operações sejam executadas, uma conta
# que sem a política poderia realizar estas operações (exemplos: put_object, delete_object,
# get_object), quando executadas num objeto que é alvo de uma política contendo o `Effect` `Deny`,
# falham com o erro `AccessDeniedByPolicy`, como demonstra o teste a seguir:

number_clients = 2
test_cases_actions_and_methods = [
    {"action": "s3:PutObject", "boto3_action": "put_object"},
    {"action": "s3:GetObject", "boto3_action": "get_object"},
    {"action": "s3:DeleteObject", "boto3_action": "delete_object"},
]
test_cases = [
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": item["action"], "effect": "Deny"}, item["boto3_action"])
    for item in test_cases_actions_and_methods
]
@pytest.mark.parametrize(
    'multiple_s3_clients, bucket_with_one_object_policy, boto3_action',
     test_cases,
     indirect = ['multiple_s3_clients', 'bucket_with_one_object_policy'],
     ids = [f"{item['action']},{item['boto3_action']}" for item in test_cases_actions_and_methods],
)
def test_denied_policy_operations_by_owner(s3_client, bucket_with_one_object_policy, boto3_action):
    bucket_name, object_key = bucket_with_one_object_policy
    kwargs = {
        'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        'Key': object_key
    }

    # PutObject expects a Body argument
    if boto3_action == 'put_object' :
        kwargs['Body'] = 'The answer for everthing is 42'

    # put_object_retention expects a Retention argument
    if boto3_action == 'put_object_retention' :
        kwargs['Retention'] = {
            "Mode": "COMPLIANCE",
            "RetainUntilDate": (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        }

    #retrieve the method passed as argument
    logging.info(f"call boto3_action:{boto3_action}, with args:{kwargs}")
    method = getattr(s3_client, boto3_action)
    try:
        response = method(**kwargs)
        logging.info(f"Method response:{response}")
        pytest.fail("Expected exception not raised")
    except ClientError as e:
        logging.info(f"Method error response:{e.response}")
        assert e.response['Error']['Code'] == 'AccessDeniedByBucketPolicy'
run_example(__name__, "test_denied_policy_operations_by_owner", config=config)

# ## Permitir operações específicas em objetos
#
# Da mesma forma, uma política pode dar um acesso a uma conta para determinadas operações. Uma
# conta que sem política normalmente seria barrada com um erro `403` para operações como put_object,
# get_object, delete_object, etc, por meio de uma política com `Effect` `Allow` conseguem obter
# sucesso (status `200`, `204` e similares) como demostra o teste abaixo:

@pytest.mark.parametrize('multiple_s3_clients, bucket_with_one_object_policy, boto3_action, expected', [
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:PutObject", "effect": "Allow", "Principal": "*"}, 'put_object', 200),
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:GetObject", "effect": "Allow", "Principal": "*"}, 'get_object', 200),
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:DeleteObject", "effect": "Allow", "Principal": "*"}, 'delete_object', 204)
], indirect = ['multiple_s3_clients', 'bucket_with_one_object_policy'])
def test_allow_policy_operations_by_owner(multiple_s3_clients, bucket_with_one_object_policy, boto3_action,expected):
    bucket_name, object_key = bucket_with_one_object_policy

    kwargs = {
        'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        'Key': object_key
    }

    #PutObject needs another variable
    if boto3_action == 'put_object' :
        kwargs['Body'] = 'The answer for everthing is 42'
        
    #retrieve the method passed as argument
    method = getattr(multiple_s3_clients[0], boto3_action)
    response = method(**kwargs)
    assert response['ResponseMetadata']['HTTPStatusCode'] == expected
run_example(__name__, "test_allow_policy_operations_by_owner", config=config)

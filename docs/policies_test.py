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
# que podem ser executadas em um bucket ou objeto, e por quais contas ("Principals"), seja para
# **conceder** mais acessos a determinados recursos, e para quais contas.

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
from botocore.exceptions import ClientError
from s3_helpers import(
    change_policies_json,
)

pytestmark = pytest.mark.policy
# -

# Políticas de bucket são descritas por meio de arquivos no formato JSON, que seguem uma gramática
# específica, devem conter uma lista de regras _Statement_ onde cada ítem desta lista descreve um
# `Effect` (`Allow` ou `Deny`), um campo `Principal` para descrever a(s) beneficiária(s) da regra
# um campo `Action` com as operações que esta regra permite ou nega e um campo `Resource`, que
# define a qual recurso esta regra se aplicará. As regras desta sintaxe pode ser consultada no
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
# O método para atribuir uma bucket policy na biblioteca boto3 é o [put_bucket_policy](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_policy.html), se o documento de política for válido o retorno trará um HTTPStatusCode `204` enquanto
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


@pytest.mark.parametrize('policies_args', [
    {"policy_dict": policy_dict_template, "actions": "s3:PutObject", "effect": "Deny"},
    {"policy_dict": policy_dict_template, "actions": "s3:GetObject", "effect": "Deny"},
    {"policy_dict": policy_dict_template, "actions": "s3:DeleteObject", "effect": "Deny"}
])
def test_setup_policies(s3_client, existing_bucket_name, policies_args):
    bucket_name = existing_bucket_name

    #given a existent and valid bucket
    policies = change_policies_json(existing_bucket_name, policies_args, "*")
    response = s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policies) 
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

# ## Negar operações específicas em objetos
#
# Regras com o `Effect` `Deny` impedem que determinadas operações sejam executadas, uma conta
# que sem a política poderia realizar estas operações (exemplos: put_object, delete_object,
# get_object), quando executadas num objeto que é alvo de uma política contendo o `Effect` `Deny`,
# falham com o erro `AccessDeniedByPolicy`, como demonstra o teste a seguir:

number_clients = 2
@pytest.mark.parametrize('multiple_s3_clients, bucket_with_one_object_policy, boto3_action', [
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:PutObject", "effect": "Deny"}, 'put_object'),
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:GetObject", "effect": "Deny"}, 'get_object'),
    ({"number_clients": number_clients}, {"policy_dict": policy_dict_template, "actions": "s3:DeleteObject", "effect": "Deny"}, 'delete_object')
], indirect = ['multiple_s3_clients', 'bucket_with_one_object_policy'])
def test_denied_policy_operations_by_owner(s3_client, bucket_with_one_object_policy, boto3_action):
    bucket_name, object_key = bucket_with_one_object_policy
    kwargs = {
        'Bucket': bucket_name,  # Set 'Bucket' value from the variable
        'Key': object_key
    }

    #PutObject needs another variable
    if boto3_action == 'put_object' :
        kwargs['Body'] = 'The answer for everthing is 42'
        
    #retrieve the method passed as argument
    method = getattr(s3_client, boto3_action)
    try:
        response = method(**kwargs)
        logging.info(f"Method response:{response}")
        pytest.fail("Expected exception not raised")
    except ClientError as e:
        logging.info(f"Method error response:{e.response}")
        assert e.response['Error']['Code'] == 'AccessDeniedByBucketPolicy'

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

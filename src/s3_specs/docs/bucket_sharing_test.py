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

# # Presigned URL Tests
#
# Testes relacionados à criação e uso de URLs pré-assinados para buckets com diferentes operações e configurações


# + {"tags": ["parameters"]}
config = "../params/br-ne1.yaml"

# + {"jupyter": {"source_hidden": true}}
import pytest
import os
import time
import logging
import requests
import boto3
import yaml
from pathlib import Path
from s3_specs.docs.s3_helpers import run_example

pytestmark = [pytest.mark.bucket_sharing, pytest.mark.presign, pytest.mark.quick, pytest.mark.homologacao, pytest.mark.skip_if_dev]
config = os.getenv("CONFIG", config)

def get_second_profile_boto3_client(profile_name, service='s3'):
    """Cria um client boto3 para o perfil secundário"""
    second_profile = f"{profile_name}-second"
    try:
        session = boto3.Session(profile_name=second_profile)
        client = session.client(service)
        return client
    except Exception as e:
        logging.warning(f"Não foi possível criar client para perfil {second_profile}: {str(e)}")
        return None
    
def get_owner_id(profile_name):
    """Obtém o tenant ID de um perfil secundário"""
    try:
        client = get_second_profile_boto3_client(profile_name)
        if client:
            response = client.list_buckets()
            return response['Owner']['ID']
        return None
    except Exception as e:
        logging.warning(f"Não foi possível obter ID do proprietário: {str(e)}")
        return None
        
# Função para criar o arquivo LICENSE 
def create_license_file():
    """Cria ou verifica a existência do arquivo LICENSE"""
    current_dir = Path(__file__).parent.resolve()
    license_path = current_dir / "LICENSE"
    
    if not license_path.exists():
        # Criar um arquivo LICENSE temporário
        with open(license_path, 'w') as f:
            f.write("Copyright (c) 2025 Example Company\n")
            f.write("This is a test license file.\n")
    
    return license_path

def cleanup_bucket(s3_client, bucket_name):
    try:
        # Listar e excluir todos os objetos
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        # Excluir o bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} removido com sucesso")
    except Exception as e:
        logging.warning(f"Erro ao limpar bucket {bucket_name}: {str(e)}")
# -

# ### Teste 1: Criar URL Pré-assinada (GET)
#
# Este teste verifica a criação de URLs pré-assinadas para operações GET.

# +
def test_get_presigned_url(s3_client, profile_name):
    timestamp = int(time.time())
    test_bucket_name = f"test-036-{timestamp}-boto3-{profile_name}"
    file_name = "LICENSE"
    
    license_path = create_license_file()
    
    try:
        logging.info(f"Criando bucket: {test_bucket_name}")
        s3_client.create_bucket(Bucket=test_bucket_name)
        
        logging.info(f"Fazendo upload do arquivo {file_name} para o bucket")
        s3_client.upload_file(str(license_path), test_bucket_name, file_name)

        logging.info("Gerando URL pré-assinada para GET")
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': test_bucket_name, 'Key': file_name},
            ExpiresIn=300  
        )
        
        logging.info("Verificando acesso via URL pré-assinada")
        response = requests.get(presigned_url)
        assert response.status_code == 200, f"Falha ao acessar URL pré-assinada: Status {response.status_code}"
        assert "Copyright" in response.text, "Conteúdo não contém 'Copyright'"
        
    finally:
        logging.info(f"Removendo bucket: {test_bucket_name}")
        cleanup_bucket(s3_client, test_bucket_name)

run_example(__name__, "test_get_presigned_url", config=config)
# -

# ### Teste 2: Criar URL Pré-assinada para operação PUT
#
# Este teste verifica a criação e uso de URLs pré-assinadas para operações PUT.

# +
def test_put_presigned_url(s3_client, profile_name):
    """
    Testa a criação de URLs pré-assinadas para operações PUT.
    """
    timestamp = int(time.time())
    test_bucket_name = f"test-036-{timestamp}-boto3-{profile_name}"
    file_name = "LICENSE"
    new_file_name = "LICENSE_PUT_TEST"
    
    license_path = create_license_file()
    
    try:
        logging.info(f"Criando bucket: {test_bucket_name}")
        s3_client.create_bucket(Bucket=test_bucket_name)
        
        logging.info(f"Fazendo upload do arquivo {file_name} para o bucket")
        s3_client.upload_file(str(license_path), test_bucket_name, file_name)
        
        logging.info("Gerando URL pré-assinada para operação PUT")
        put_presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': test_bucket_name, 'Key': new_file_name},
            ExpiresIn=300  
        )
        
        logging.info("Fazendo upload de arquivo usando URL pré-assinada")
        with open(license_path, 'rb') as f:
            put_response = requests.put(put_presigned_url, data=f)
        
        assert put_response.status_code in [200, 204], f"Falha no upload via URL pré-assinada: Status {put_response.status_code}"
        
        logging.info("Verificando se o objeto existe no bucket")
        response = s3_client.head_object(Bucket=test_bucket_name, Key=new_file_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "O objeto não foi encontrado no bucket"
        
    finally:
        cleanup_bucket(s3_client, test_bucket_name)

run_example(__name__, "test_put_presigned_url", config=config)
# -

# ### Teste 3: Teste Consolidado de URLs Pré-assinadas ###

# +
def test_presigned_url_scenarios(s3_client, profile_name):
    """
    Teste unificado que cobre todos os cenários de URLs pré-assinadas:
    1. Bucket público
    2. Bucket privado padrão 
    3. Bucket com ACL específica (executado apenas se existir perfil secundário)
    """
    timestamp = int(time.time())
    file_name = "LICENSE"
    license_path = create_license_file()
    endpoint_url = boto3.Session(profile_name=profile_name)._session.get_scoped_config().get('endpoint_url')

    # ---- 1. Teste Bucket Público ----
    public_bucket = f"test-pub-{timestamp}-{profile_name}"
    try:
        s3_client.create_bucket(Bucket=public_bucket)
        s3_client.put_bucket_acl(Bucket=public_bucket, ACL='public-read')
        s3_client.upload_file(str(license_path), public_bucket, file_name)
        
        response = requests.get(f"{endpoint_url}{public_bucket}")
        assert response.status_code == 200, "Falha ao acessar bucket público"
        assert "ListBucketResult" in response.text, "Resposta não contém ListBucketResult"
        
    finally:
        cleanup_bucket(s3_client, public_bucket)

    # ---- 2. Teste Bucket Privado ----
    private_bucket = f"test-priv-{timestamp}-{profile_name}"
    try:
        s3_client.create_bucket(Bucket=private_bucket)
        s3_client.upload_file(str(license_path), private_bucket, file_name)
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': private_bucket, 'Key': file_name},
            ExpiresIn=300
        )
        
        assert "X-Amz-Algorithm" in url, "URL não contém assinatura válida"
        response = requests.get(url)
        assert response.status_code == 200, "Falha ao acessar URL pré-assinada"
        assert "Copyright" in response.text, "Conteúdo do arquivo incorreto"
        
    finally:
        cleanup_bucket(s3_client, private_bucket)

    # ---- 3. Teste ACL Específica ----
    second_user_id = get_owner_id(profile_name)
    if not second_user_id:
        pytest.skip(f"Teste de ACL ignorado: Perfil secundário '{profile_name}-second' não encontrado")
    else:
        acl_bucket = f"test-acl-{timestamp}-{profile_name}"
        try:
            s3_client.create_bucket(Bucket=acl_bucket)
            s3_client.put_bucket_acl(
                Bucket=acl_bucket,
                GrantRead=f"id={second_user_id}",
                GrantWrite=f"id={second_user_id}"
            )
            s3_client.upload_file(str(license_path), acl_bucket, file_name)
            
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': acl_bucket, 'Key': file_name},
                ExpiresIn=300
            )
            
            response = requests.get(url)
            assert response.status_code == 200, "Falha ao acessar URL com ACL"
            assert "Copyright" in response.text, "Conteúdo do arquivo incorreto"

        finally:
            cleanup_bucket(s3_client, acl_bucket)

run_example(__name__, "test_presigned_url_scenarios", config=config)
# -


# ## Referências
#
# - [Boto3 S3 generate_presigned_url](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url)
# - [AWS S3 Presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-urls.html)
# - [Boto3 S3 put_bucket_acl](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_acl)
# - [AWS S3 ACL Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html)
# - [Python requests](https://docs.python-requests.org/en/latest/)
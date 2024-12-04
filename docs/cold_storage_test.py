# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # Cold Storage
#
# Por padrão, novos objetos utilizam a classe de armazenamento "standard", que é adequada para acessos frequentes.
# No entanto, para muitos casos, o acesso aos objetos armazenados pode ser raro e o principal objetivo é garantir que eles
# sejam mantidos por longos períodos, ainda que disponíveis para acesso rápido quando necessário.
#
#  Exemplos: backups, logs, registros arquivados para cumprimento de legislações.

# Para esses casos, nas regiões suportadas, está disponível a classe de armazenamento fria "cold_instant",
# que oferece um custo de armazenamento reduzido e um custo de acesso mais elevado. Consulte a página de Preços
# para comparar as diferentes classes de armazenamento.

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import boto3
import pytest
import logging
import subprocess
import json
import os
from time import sleep
from shlex import split, quote
from s3_helpers import run_example, get_spec_path
from datetime import datetime, timedelta, timezone
# -

# ## Exemplos


# ### Trocar a classe de um objeto
#
# Para trocar a classe de um objeto para a classe de armazenamento cold storage, utilize o comando
# mgc object-storage objects copy NOME_DO_BUCKET/NOME_DO_OBJETO MESMO_NOME_DO_BUCKET/MESMO_NOME_DO_OBJETO --storage-class=cold_instant

# Para realizar o upload de um objeto com a classe de armazenamento cold storage, utilize o comando
# mgc object-storage objects upload ARQUIVO NOME_DO_BUCKET --storage-class=cold_instant

# + tags=["parameters"]
config = "../params/aws-east-1.yaml"
# -
profile = config.split('/')[2].split('.')[0]

LOGGER = logging.getLogger(__name__)

# commands = [
#     "mgc object-storage objects upload {object_key} {bucket_name} --storage-class=cold_instant",
#     "aws --profile {profile} s3 cp {object_key} s3://{bucket_name} --storage-class=GLACIER_IR",
#     "aws --profile {profile} s3api put-object --bucket {bucket_name} --key {object_key}",
#     "rclone settier 'cold_instant' '{profile}:{bucket_name}/{object_key}'"
# ]

# # + {"jupyter": {"source_hidden": true}}
# @pytest.mark.parametrize("cmd_template", commands)
# def test_upload_object_with_cold_storage_class(cmd_template, empty_bucket_name):
#     bucket_name = empty_bucket_name
#     object_key = "file.txt"
#     content = "Arquivo teste para upload com a classe de armazenamento Cold Storage"

#     with open("file.txt", "w") as file:
#         file.write(content)
#         file.close()

#     cmd = split(cmd_template.format(profile="br-se1", object_key=object_key, bucket_name=bucket_name, content=content))

#     result = subprocess.run(cmd, capture_output=True, text=True)

#     assert result.returncode == 0, f"Command failed with error: {result.stderr}"
#     logging.info(f"Output from {cmd_template}: {result.stdout}")

#     os.remove('file.txt')

# run_example(__name__, "test_upload_object_with_cold_storage_class", config=config)

def test_boto_upload_object_with_cold_storage_class(s3_client, empty_bucket_name):
    # Configuração inicial
    bucket_name = empty_bucket_name
    object_key = "cold_file.txt"
    content = "Arquivo de exemplo com a classe cold storage"

    # Teste de upload
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass="GLACIER_IR" 
    )

    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info("Status do upload: %s", response_status)
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful upload."
    
    storage_class = s3_client.get_object_attributes(Bucket=bucket_name, Key=object_key, ObjectAttributes = ["StorageClass"])
    
    storage_class = storage_class.get("StorageClass")
    logging.info("StorageClass: %s", storage_class)
    
    assert storage_class == "GLACIER_IR" or storage_class == "COLD_INSTANT", "Expected StorageClass GLACIER_IR or COLD_INSTANT"
    
run_example(__name__, "test_boto_upload_object_with_cold_storage_class", config=config)

def test_boto_change_object_class_to_cold_storage(s3_client, empty_bucket_name):
    # Configuração inicial
    bucket_name = empty_bucket_name
    object_key = "cold_file.txt"
    content = "Arquivo de exemplo com a classe cold storage"

    # Teste de upload
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass="STANDARD"
    )
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info("Response Status: %s", response_status)
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful upload."
    
    storage_class = s3_client.get_object_attributes(Bucket=bucket_name, Key=object_key, ObjectAttributes = ["StorageClass"]).get("StorageClass")
    logging.info("StorageClass: %s", storage_class)
    
    assert storage_class == "STANDARD", "Expected StorageClass STANDARD"
    
    response = s3_client.copy_object(
        Bucket=bucket_name,
        CopySource=f"{bucket_name}/{object_key}",
        Key=object_key,
        StorageClass="GLACIER_IR"  # Substitua pela classe desejada
    )
    logging.info("Response Status: %s", response["ResponseMetadata"]["HTTPStatusCode"])
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "Expected HTTPStatusCode 200 for successful copy."

    storage_class = s3_client.get_object_attributes(Bucket=bucket_name, Key=object_key, ObjectAttributes = ["StorageClass"]).get("StorageClass")
    logging.info("StorageClass: %s", storage_class)
    
    assert storage_class == "GLACIER_IR" or storage_class == "COLD_INSTANT", "Expected StorageClass GLACIER_IR or COLD_INSTANT"

run_example(__name__, "test_boto_change_object_class_to_cold_storage", config=config)

def test_boto_object_with_custom_metadata_and_storage_class(s3_client, empty_bucket_name):
    # Configuração inicial
    bucket_name = empty_bucket_name
    object_key = "cold_file.txt"
    content = "Arquivo de exemplo com a classe cold storage"

    # Teste de upload
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass="GLACIER_IR",
        Metadata={
            "metadata1":"foo",
            "metadata2":"bar"
        }
    )

    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info("Response Status: %s", response_status)
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful upload."
    
    head = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    storage_class = head.get('StorageClass')
    metadata = head.get('Metadata')

    logging.info("Metadata: %s", metadata)
    assert "metadata1" in list(metadata.keys()), "Expected metadata1 as a custom metadata"

    logging.info("StorageClass: %s", storage_class)
    assert storage_class == "GLACIER_IR" or storage_class == "COLD_INSTANT", "Expected StorageClass GLACIER_IR or COLD_INSTANT"
    
run_example(__name__, "test_boto_object_custom_metadata_with_storage_class", config=config)

def test_boto_multipart_upload_with_cold_storage_class(s3_client, empty_bucket_name, create_big_file_with_two_parts):
    bucket_name = empty_bucket_name

    object_key, part_files = create_big_file_with_two_parts

    # Criação do arquivo de aproximadamente 50 MB

    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        StorageClass="GLACIER_IR",
    )

    assert "UploadId" in list(response.keys())
    upload_id = response.get("UploadId")
    logging.info("Upload Id: %s", upload_id)
    logging.info("Create Multipart Upload Response: %s", response)

    parts = []
    for i, part_file in enumerate(part_files, start=1):
        with open(part_file, "rb") as part_content:
            response_part = s3_client.upload_part(
                Body=part_content,
                Bucket=bucket_name,
                Key=object_key,
                PartNumber=i,
                UploadId=upload_id,
            )
            parts.append({'ETag': response_part['ETag'], 'PartNumber': i})
            logging.info("Response Upload Part %s: %s", i, response_part)
            assert response_part["ResponseMetadata"]["HTTPStatusCode"] == 200, (
                f"Expected HTTPStatusCode 200 for part {i} upload."
            )

    response = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        MultipartUpload={'Parts': parts},
        UploadId=upload_id,
    )
    
    logging.info("Complete Multipart Upload Response: %s", response)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, (
                f"Expected HTTPStatusCode 200 for part {i} upload."
            )
    
    head = s3_client.head_object(
        Bucket= bucket_name,
        Key = object_key
    )
    logging.info("Storage Class: %s", head.get("StorageClass"))
    assert head.get("StorageClass") == "GLACIER_IR" or head.get("StorageClass") == "COLD_INSTANT", "Expected StorageClass GLACIER_IR or COLD_INSTANT"

run_example(__name__, "test_boto_multipart_upload_with_cold_storage_class", config=config)

def test_boto_list_objects_with_cold_storage_class(s3_client, bucket_with_one_object_and_cold_storage_class):
    bucket_name, _, _ = bucket_with_one_object_and_cold_storage_class

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    logging.info("Response list: %s", response)
    assert len(response.get('Contents')) == 1, "Expected returning one object"
    
    obj = response.get('Contents')[0]
    logging.info("Object info: %s", obj)

    obj_storage_class = obj.get('StorageClass')
    logging.info("Object class: %s", obj_storage_class)

    assert obj_storage_class == 'COLD_INSTANT' or obj_storage_class == 'GLACIER_IR', "Expected GACIER_IR or COLD_INSTANT as Storage Class"

run_example(__name__, "test_boto_multipart_upload_with_cold_storage_class", config=config)

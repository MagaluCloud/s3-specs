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

# # Cold Storage
#
# Por padrão, novos objetos utilizam a classe de armazenamento "standard", que é adequada para acessos frequentes.
# No entanto, para muitos casos, o acesso aos objetos armazenados pode ser raro e o principal objetivo é garantir que eles
# sejam mantidos por longos períodos, ainda que disponíveis para acesso rápido quando necessário.
#
#  Exemplos: backups, logs, registros arquivados para cumprimento de legislações.

# Para esses casos, na região br-se1 do magalu cloud, está disponível a classe de armazenamento fria "cold_instant",
# que oferece um custo de armazenamento reduzido e um custo de acesso mais elevado. Consulte a página de Preços
# para comparar as diferentes classes de armazenamento.

# + {"tags": ["parameters"]}
config = "../../../params/br-se1.yaml"

# + {"jupyter": {"source_hidden": true}}
import logging
import pytest
from s3_specs.docs.s3_helpers import run_example
from botocore.exceptions import ClientError
import hashlib

pytestmark = pytest.mark.cold_storage
# -

# ## Exemplos

# + {"tags": ["parameters"]}
config = "../../../params/aws-east-1.yaml"
# -

# ### Subindo um objeto utilizando boto3
#
# O Magalu Cloud possibilita a utilização da biblioteca Python boto3 para manipular buckets e seus objetos.  
# No exemplo abaixo, é demonstrado como realizar o upload de um objeto novo em um bucket já configurado com a classe fria.  
# O boto3 aceita apenas algumas classes específicas como parâmetro, como "STANDARD", "GLACIER_IR", etc.  
#
# No Magalu Cloud, a classe fria é chamada **COLD_INSTANT**, mas, por motivos de compatibilidade com o boto3, utiliza-se a classe **GLACIER_IR** para especificar um objeto com a classe fria.

# +
def test_upload_object_with_cold_storage_class(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name
    object_key = "cold_file.txt"
    content = "Arquivo de exemplo com a classe cold storage"

    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass="GLACIER_IR" 
    )

    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info("Status do upload: %s", response_status)
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful upload."
    
    storage_class = s3_client.get_object_attributes(Bucket=bucket_name, Key=object_key, ObjectAttributes = ["StorageClass"]).get("StorageClass")
    logging.info("StorageClass: %s", storage_class)
    
    assert storage_class == "GLACIER_IR" or storage_class == "COLD_INSTANT", "Expected StorageClass GLACIER_IR or COLD_INSTANT"
    
run_example(__name__, "test_upload_object_with_cold_storage_class", config=config)
# -

# ### Trocar a classe de um objeto existente    
#
# Além de poder subir um novo objeto com a classe, também é possível trocar a classe de armazenamento 
# de um objeto existente, usando a função copy_object do boto3. Isso é feito copiando um objeto para o mesmo lugar 
# (mesma object key), mas passando um valor diferente para o argumento StorageClass, como é possível visualizar no exemplo abaixo.

# +
def test_change_object_class_to_cold_storage(s3_client, bucket_with_one_object):
    bucket_name, object_key, _ = bucket_with_one_object

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

run_example(__name__, "test_change_object_class_to_cold_storage", config=config)
# -

# ### Upload de um objeto com metadados customizados e ACLs
#
# O upload de um objeto com storage class não é diferente de um upload normal 
# e aceita todos os outros atributos possíveis. O exemplo abaixo demonstra o upload de um objeto
# com metadados customizados, ACLs e a classe fria.

# +
def test_object_with_custom_metadata_acls_and_cold_storage_class(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name
    object_key = "cold_file.txt"
    content = "Arquivo de exemplo com a classe cold storage"

    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        StorageClass="GLACIER_IR",
        Metadata={
            "metadata1":"foo",
            "metadata2":"bar"
        },
        ACL="public-read"
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
    
    acl = s3_client.get_object_acl(
        Bucket=bucket_name,
        Key=object_key,
    ).get('Grants')[0]
    
    assert 'READ' == acl.get('Permission'), "Expected acl permission be READ"

run_example(__name__, "test_object_with_custom_metadata_acls_and_storage_class", config=config)
# -

# ### Listagem de objetos
#
# No boto3, utilizando a função list_objects_v2, é possível listar os objetos de um bucket e, junto do objeto, 
# obter algumas informações sobre ele, sendo uma delas a StorageClass. Apesar de não ser compatível 
# utilizar a classe COLD_INSTANT durante o put_object ou copy_object com o Magalu Cloud, quando se realiza a busca 
# de um objeto ou das informações do objeto no Magalu Cloud, a classe retornada é COLD_INSTANT.

# +
def test_list_objects_with_cold_storage_class(s3_client, bucket_with_one_storage_class_cold_object):
    bucket_name, _, _ = bucket_with_one_storage_class_cold_object

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    logging.info("Response list: %s", response)
    assert len(response.get('Contents')) == 1, "Expected returning one object"
    
    obj = response.get('Contents')[0]
    logging.info("Object info: %s", obj)

    obj_storage_class = obj.get('StorageClass')
    logging.info("Object class: %s", obj_storage_class)

    assert obj_storage_class == 'COLD_INSTANT' or obj_storage_class == 'GLACIER_IR', "Expected GACIER_IR or COLD_INSTANT as Storage Class"

run_example(__name__, "test_multipart_upload_with_cold_storage_class", config=config)
# -

# ### Multipart Upload com a Classe Fria
#
# Outra possibilidade é realizar o multipart upload utilizando a classe fria. 

# +
def test_multipart_upload_with_cold_storage_class(s3_client, existing_bucket_name, create_multipart_object_files):
    bucket_name = existing_bucket_name

    object_key,_, part_bytes = create_multipart_object_files

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
    for i, part_content in enumerate(part_bytes, start=1):
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

    list_parts_response = s3_client.list_parts(
        Bucket=bucket_name,
        Key=object_key,
        UploadId=upload_id,
    ).get("Parts")

    logging.info("List parts: %s", list_parts_response)
    assert len(list_parts_response) == 2, "Expected list part return has the same size of interaction index"

    list_parts_etag = [part.get("ETag") for part in list_parts_response]
    assert response_part.get("ETag") in list_parts_etag, "Expected ETag being equal"


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

run_example(__name__, "test_multipart_upload_with_cold_storage_class", config=config)


def test_multipart_upload_versioned_with_cold_storage_class(s3_client, versioned_bucket_with_one_object_cold_storage_class, create_multipart_object_files):
    bucket_name, object_key, _ = versioned_bucket_with_one_object_cold_storage_class

    object_key,_, part_bytes = create_multipart_object_files

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
    for i, part_content in enumerate(part_bytes, start=1):
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

    list_parts_response = s3_client.list_parts(
        Bucket=bucket_name,
        Key=object_key,
        UploadId=upload_id,
    ).get("Parts")

    logging.info("List parts: %s", list_parts_response)
    assert len(list_parts_response) == 2, "Expected list part return has the same size of interaction index"

    list_parts_etag = [part.get("ETag") for part in list_parts_response]
    assert response_part.get("ETag") in list_parts_etag, "Expected ETag being equal"


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

run_example(__name__, "test_multipart_upload_versioned_with_cold_storage_class", config=config)

def test_delete_bucket_with_objects_cold_storage_class_and_versions(s3_client, versioned_bucket_with_one_object_cold_storage_class):
    bucket_name, object_key, _ = versioned_bucket_with_one_object_cold_storage_class

    s3_client.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = b"v2",
        StorageClass='GLACIER_IR'
    )

    with pytest.raises(ClientError, match="BucketNotEmpty") as exc_info:
        s3_client.delete_bucket(
            Bucket=bucket_name,
        )

    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code == "BucketNotEmpty"

run_example(__name__, "test_delete_bucket_with_objects_cold_storage_class_and_versions", config=config)

def test_multipart_upload_checksum_validation(s3_client, versioned_bucket_with_one_object_cold_storage_class, create_multipart_object_files):
    bucket_name, object_key, _ = versioned_bucket_with_one_object_cold_storage_class
    object_key, _, part_bytes = create_multipart_object_files

    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        StorageClass="GLACIER_IR",
    )

    assert "UploadId" in list(response.keys())
    upload_id = response.get("UploadId")
    logging.info("Upload Id: %s", upload_id)

    parts = []
    for i, part_content in enumerate(part_bytes, start=1):
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
                f"Expected HTTPStatusCode 200 for multipart upload completion."
            )

    upload_checksum = calculate_checksum(b"".join(part_bytes))
    logging.info(f"Calculated upload checksum: {upload_checksum}")

    response_get = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )

    downloaded_checksum = calculate_checksum(response_get['Body'].read())
    logging.info(f"Calculated downloaded checksum: {downloaded_checksum}")

    assert upload_checksum == downloaded_checksum, (
        f"Checksum mismatch: {upload_checksum} (uploaded) vs {downloaded_checksum} (downloaded)"
    )

    logging.info("Checksum validation passed: The uploaded and downloaded content match.")


def calculate_checksum(file_content):
    """Calcula o MD5 do conteúdo do arquivo."""
    hash_md5 = hashlib.md5()
    hash_md5.update(file_content)
    return hash_md5.hexdigest()

import pytest
import logging
import hashlib

@pytest.fixture
def fixture_multipart_upload_cold(s3_client, versioned_bucket_with_one_object_cold_storage_class, create_multipart_object_files):
    """Fixture que cria um upload multipart e retorna as informações necessárias para o teste."""

    # Recupera o nome do bucket, a chave do objeto e versão do objeto do bucket versionado com a classe de armazenamento 'COLD_STORAGE_CLASS'.
    bucket_name, object_key, _ = versioned_bucket_with_one_object_cold_storage_class
    object_key, _, part_bytes = create_multipart_object_files

    # Cria o upload multipart com a classe de armazenamento 'GLACIER_IR'.
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        StorageClass="GLACIER_IR",
    )

    assert "UploadId" in list(response.keys())
    upload_id = response.get("UploadId")
    logging.info("Upload Id: %s", upload_id)

    parts = []
    # Realiza o upload das partes.
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

    # Lista as partes do upload e verifica se o número de partes está correto.
    list_parts_response = s3_client.list_parts(
        Bucket=bucket_name,
        Key=object_key,
        UploadId=upload_id,
    ).get("Parts")

    logging.info("List parts: %s", list_parts_response)
    assert len(list_parts_response) == 2, "Expected list part return has the same size of interaction index"

    # Completa o upload multipart.
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

    return bucket_name, object_key, upload_id, part_bytes  # Retorna as variáveis necessárias para o teste

@pytest.fixture
def calculate_checksum_fixture():
    """Fixture que calcula o MD5 do conteúdo do arquivo."""
    def _calculate(file_content):
        hash_md5 = hashlib.md5()
        hash_md5.update(file_content)
        return hash_md5.hexdigest()
    
    return _calculate

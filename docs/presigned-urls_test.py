# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # URLs pré-assinadas (Presigned URLs)
#
# Para realizar o upload e download de objetos em um bucket S3, normalmente são necessárias
# credenciais de acesso (API key e secret key), pois essas operações precisam ser autenticadas.
# No entanto, existe uma maneira de criar URLs temporárias e seguras para operações específicas,
# como PUT (upload) ou GET (download), sem a necessidade de compartilhar as credenciais. Essas
# são as URLs pré-assinadas (presigned URLs).
#
# URLs pré-assinadas são usadas em cenários de upload direto, permitindo que um cliente
# envie arquivos diretamente para o S3, sem que a aplicação precise expor suas credenciais de acesso.

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
# -

# + {"jupyter": {"source_hidden": true}}
import pytest
import logging
from s3_helpers import run_example, delete_object_and_wait
import requests
import os

pytestmark = [pytest.mark.presign, pytest.mark.prod]
config = os.getenv("CONFIG", config)
# -

# ## Exemplos

# ### Presigned GET URL
#
# Gera uma URL para download (GET) de um objeto existente.
# Posteriormente faz o download deste objeto através da URL temporária gerada.

# +
def test_presigned_get_url(s3_client, bucket_with_one_object):
    bucket_name, object_key, content = bucket_with_one_object

    # Generate a presigned GET URL for the object
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket_name, "Key": object_key},
        ExpiresIn=3600
    )

    # Assert that the presigned URL is generated
    assert presigned_url, "Failed to generate presigned GET URL."
    print(f"Presigned GET URL created: {presigned_url}")

    # Use the presigned URL to retrieve the object
    print("Starting GET request...")
    response = requests.get(presigned_url)

    # Assertions to confirm retrieval success and content match
    assert response.status_code == 200, f"GET request failed with status code {response.status_code}"
    assert response.content == content, "Downloaded content does not match the original content."
    print(f"Object downloaded: {content}")

run_example(__name__, "test_presigned_get_url", config=config)
# -

# ### Presigned PUT URL
#
# Gera uma URL para upload (PUT) de um objeto e utiliza essa URL para enviar dados ao bucket S3.

# +
def test_presigned_put_url(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name

    # Define the object key and content
    object_key = "test-upload-object.txt"
    content = b"Sample content for presigned PUT test."

    # Generate a presigned PUT URL
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket_name, "Key": object_key},
        ExpiresIn=3600
    )

    # Assert that the presigned URL is generated
    assert presigned_url, "Failed to generate presigned PUT URL."
    print(f"Presigned PUT URL created: {presigned_url}")

    # Use the presigned URL to upload the content
    print("Starting PUT request...")
    response = requests.put(presigned_url, data=content)

    # Assert the upload succeeded
    assert response.status_code == 200, f"PUT request failed with status code {response.status_code}"

    # Verify the object exists and its content matches
    head_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    assert head_response["ContentLength"] == len(content), "Uploaded content size mismatch."
    print(f"Object '{object_key}' uploaded successfully and content verified.")

    # Cleanup: delete the uploaded object
    delete_object_and_wait(s3_client, bucket_name, object_key)

run_example(__name__, "test_presigned_put_url", config=config)
# -

# ## Outros exemplos
#
# ### Gerar uma URL pré-assinada para PutObject que já suba o objeto com uma canned-acl de "public-read":

# +
def test_presigned_put_url_with_acl(s3_client, existing_bucket_name):
    bucket_name = existing_bucket_name

    # Define the object key and content
    object_key = "test-upload-object.txt"
    content = b"Sample content for presigned PUT, with ACL, test."
    acl = "public-read"

    # Generate a presigned PUT URL
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": bucket_name,
            "Key": object_key,
            "ACL": acl,
        },
        ExpiresIn=3600,
    )

    # Assert that the presigned URL is generated
    assert presigned_url, "Failed to generate presigned PUT URL."
    logging.info(f"Presigned PUT URL created: {presigned_url}")

    # Use the presigned URL to upload the content
    headers = { "host": presigned_url.split("/")[2], "x-amz-acl": acl }
    logging.info(f"Starting PUT request to {presigned_url} with headers {headers}")

    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)
    response = requests.put(presigned_url, data=content, headers=headers)

    # Assert the upload succeeded
    logging.info(f"response: {response}")
    assert response.status_code == 200, f"PUT request failed with status code {response.status_code}"

    # Verify the object exists and its content matches
    head_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    assert head_response["ContentLength"] == len(content), "Uploaded content size mismatch."
    logging.info(f"Object '{object_key}' uploaded successfully and content length verified.")

    # public URL of the uploaded object
    public_url = presigned_url.split("?")[0]

    # Verify that a public-read object uploaded with this presigned URL is downloable
    # Use the presigned URL to retrieve the object
    logging.info(f"Starting GET request to {public_url}")
    response = requests.get(public_url)

    # Assertions to confirm retrieval success and content match
    assert response.status_code == 200, f"GET request failed with status code {response.status_code}"
    assert response.content == content, "Downloaded content does not match the original content."
    logging.info(f"Object downloaded: {content}")

    # Cleanup: delete the uploaded object
    delete_object_and_wait(s3_client, bucket_name, object_key)

run_example(__name__, "test_presigned_put_url", config=config)
# -


# ## Referências:
# - [Boto3 Documentation: Presigned URLs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html)
# - [Sharing objects with presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)


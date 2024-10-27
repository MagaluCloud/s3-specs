# URLs pré-assinadas (Presigned URLs)

Para realizar o upload e download de objetos em um bucket S3, normalmente são necessárias
credenciais de acesso (API key e secret key), pois essas operações precisam ser autenticadas.
No entanto, existe uma maneira de criar URLs temporárias e seguras para operações específicas,
como PUT (upload) ou GET (download), sem a necessidade de compartilhar as credenciais. Essas
são as URLs pré-assinadas (presigned URLs).

URLs pré-assinadas são usadas em cenários de upload direto, permitindo que um cliente
envie arquivos diretamente para o S3, sem que a aplicação precise expor suas credenciais de acesso.


```python
profile_name = "default"
docs_dir = "."
```


```python
# Parameters
profile_name = "br-ne1"
docs_dir = "./docs"

```


```python
import pytest
import requests
from s3_helpers import delete_object_and_wait
def run_example(example_name):
    if __name__ == "__main__":
        pytest.main(["-qq", "-s", f"{docs_dir}/presigned-urls_test.py::{example_name}"])
```

## Exemplos

### Presigned GET URL

Gera uma URL para download (GET) de um objeto existente.
Posteriormente faz o download deste objeto através da URL temporária gerada.


```python
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

run_example("test_presigned_get_url")
```

    Presigned GET URL created: https://br-ne1.magaluobjects.com/fixture-bucket-074886/test-object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=e5402a7c-b0c8-45a3-a18e-3c39de9ca061%2F20241027%2Fbr-ne1%2Fs3%2Faws4_request&X-Amz-Date=20241027T052704Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=2f480cd610016a3ae09f0e77960ad1be3ed616d1c36300ca9d3aab2c6a183357
    Starting GET request...


    Object downloaded: b'Sample content for testing presigned URLs.'
    [32m.[0m

    


### Presigned PUT URL

Gera uma URL para upload (PUT) de um objeto e utiliza essa URL para enviar dados ao bucket S3.


```python
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

run_example("test_presigned_put_url")
```

    Presigned PUT URL created: https://br-ne1.magaluobjects.com/existing-bucket-04196e/test-upload-object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=e5402a7c-b0c8-45a3-a18e-3c39de9ca061%2F20241027%2Fbr-ne1%2Fs3%2Faws4_request&X-Amz-Date=20241027T052707Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=1d04ca07979def4748c4a351e68773dc3c8fe9895a480fa39bed98826209b44f
    Starting PUT request...


    Object 'test-upload-object.txt' uploaded successfully and content verified.


    [32m.[0m

    


## Referências:
- [Boto3 Documentation: Presigned URLs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html)
- [Sharing objects with presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)

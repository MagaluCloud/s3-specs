import pytest
import logging
from s3_specs.docs.tools.crud import (fixture_bucket_with_name,
                        fixture_upload_multiple_objects,
                        upload_multiple_objects,
                        download_objects_multithreaded,
                        list_all_objects)

### Fazendo o upload de grandes quantidades de objetos em paralelo

# O upload de grandes volumes de objetos é demorado, assim o método ideal para esta operação é o paralelismo.
# A função abaixo faz o upload de N objects para um bucket S3 e então os deleta.

# Multiple objects test data
pytestmark = [pytest.mark.skip_if_dev]

objects_number = [100, 1_000, 10_000]
file_path = "../AUTHORS"

test_ids = [
    f"num={num}" for num in objects_number
]

@pytest.mark.parametrize(
    'object_quantity, file_path',
    [(num, file_path) for num in objects_number],
    ids=test_ids,
)


@pytest.mark.slow # Mark indicating the test expected speed (slow)
@pytest.mark.multiple_objects
def test_upload_multiple_objects(s3_client, fixture_bucket_with_name, file_path: str, object_quantity: int):
    """
    Test to upload multiple objects to an S3 bucket in parallel based on the wanted quantity
    :param s3_client: pytest.fixture of boto3 s3 client
    :param fixture_bucket_with_name: pytest.fixture to create a bucket with a unique name
    :param file_path: str: path to the file to be uploaded
    :param object_quantity: int: number of objects to be uploaded
    :return: None
    """

    object_prefix = "test-multiple-small"
    bucket_name = fixture_bucket_with_name
   
    successful_uploads = upload_multiple_objects(s3_client, bucket_name, file_path, object_prefix, object_quantity)
    # Checking if all the objects were uploaded
    objects_in_bucket = len(list_all_objects(s3_client, bucket_name))

    logging.info(f"Uploaded expected: {object_quantity}, made:{successful_uploads}, bucket: {objects_in_bucket}")
    assert successful_uploads == objects_in_bucket , f"Expects uploads {successful_uploads} to be equal to objects in the bucket {objects_in_bucket} "



# # test_download_multiple_objects: Test download multiple objects, it uploads a big volume and then tries to download them, if it fails the test do as well

@pytest.mark.parametrize(
    'fixture_upload_multiple_objects',
    [{"quantity": num, "path":file_path} for num in objects_number],
    ids=test_ids,
    indirect=['fixture_upload_multiple_objects']
)


@pytest.mark.slow # Mark indicating the test expected speed (slow)
@pytest.mark.multiple_objects
def test_download_multiple_objects(s3_client, fixture_bucket_with_name, fixture_upload_multiple_objects):
    """
    Test to download multiple objects from an S3 bucket in parallel
    :param s3_client: pytest.fixture of boto3 s3 client
    :param fixture_bucket_with_name: pytest.fixture to upload objects into a s3 bucket
    :return: None
    """

    successful_uploads = fixture_upload_multiple_objects

    logging.info(f"Downloading objects from {fixture_bucket_with_name}")
    successful_downloads = download_objects_multithreaded(s3_client, fixture_bucket_with_name)

    # Checking if all the objects were downloaded
    assert successful_downloads == successful_uploads, f"Expects downloads {successful_downloads} to be equal to uploads {successful_uploads} "



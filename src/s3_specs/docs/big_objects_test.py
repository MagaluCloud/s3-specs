import pytest
import logging
from s3_specs.docs.tools.utils import fixture_create_big_file
from s3_specs.docs.tools.crud import fixture_bucket_with_name, upload_multipart_file
from boto3.s3.transfer import TransferConfig
import uuid
from tqdm import tqdm
import os

pytestmark = [pytest.mark.skip_if_dev]

size_list = [
    {'size': 10, 'unit': 'mb'},
    {'size': 100, 'unit': 'mb'},
    {'size': 1, 'unit': 'gb'},
    {'size': 5, 'unit': 'gb'},
    {'size': 10, 'unit': 'gb'},
]

ids_list = [f"{s['size']}{s['unit']}" for s in size_list]


@pytest.mark.parametrize(
    'fixture_create_big_file',
    [size for size in size_list],  
    ids=ids_list,
    indirect=['fixture_create_big_file']
)

# ## Test multipart download while implicitly tests the upload and delete of big objects

@pytest.mark.slow
@pytest.mark.big_objects
#@pytest.mark.skip(reason="Not working")
def test_multipart_download(s3_client, fixture_bucket_with_name, fixture_create_big_file):
    # Setup
    bucket_name = fixture_bucket_with_name
    file_path, total_size = fixture_create_big_file
    object_key = os.path.split(file_path)[-1] # The object key is the file name
    download_path = os.path.join(os.path.dirname(file_path), f"downloaded_{object_key}")

    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=40 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    # Uploading the big file upload_multipart_file
    try:
        uploaded_file_size = upload_multipart_file(s3_client, bucket_name, object_key, file_path, config)
    except Exception as e:
        logging.error(f"Error uploading object {object_key}: {e}")
        pytest.fail(f"Upload failed: {e}")

    # Test download file from s3 bucket
    try:
        # Graphing the download progress
        with tqdm(total=total_size, 
                  desc=bucket_name, 
                  bar_format="Download| {percentage:.1f}%|{bar:25} | {rate_fmt} | Time: {elapsed} | {desc}",  
                  unit='B', 
                  unit_scale=True, unit_divisor=1024) as pbar:

            s3_client.download_file(Bucket=bucket_name, Key=object_key, Filename = download_path, Config=config, Callback=pbar.update)  

            # Retrieving sizes
            downloaded_file_size = os.path.getsize(download_path)

            # The test was successful only if the size on the bucket size is equal to the ones uploaded and downloaded
            assert downloaded_file_size == uploaded_file_size, f"Downloaded size doesn't match: {downloaded_file_size} with Upload size: {uploaded_file_size}"
    except Exception as e:
        logging.error(f"Error downloading object {object_key}: {e}")

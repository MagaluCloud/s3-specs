import pytest
import logging
from utils.utils import create_big_file
from utils.crud import fixture_bucket_with_name, fixture_upload_multipart_file
from boto3.s3.transfer import TransferConfig
import uuid
from tqdm import tqdm
import os


size_list = [
    {'size': 10, 'unit': 'mb'},
    {'size': 100, 'unit': 'mb'},
    {'size': 1, 'unit': 'gb'},
    {'size': 5, 'unit': 'gb'},
    {'size': 10, 'unit': 'gb'},
]

ids_list = [f"{s['size']}{s['unit']}" for s in size_list]

upload_params = [
    {
        'file_path': f"/tmp/big_file_download{size['size']}{size['unit']}",
        'file_size': size,
        'object_key': "big-object-" + uuid.uuid4().hex[:6],
    }
    for size in size_list
]

@pytest.mark.parametrize(
    'params, fixture_upload_multipart_file',
    [(p, p) for p in upload_params],  
    ids=ids_list,
    indirect=['fixture_upload_multipart_file']
)

# ## Test multipart download while implicitly tests the upload and delete of big objects

@pytest.mark.slow
@pytest.mark.big_objects
def test_multipart_download(s3_client, fixture_bucket_with_name, fixture_upload_multipart_file, params):
    """
    Test to download a big object to an S3 bucket using multipart download
    :param s3_client: fixture of boto3 s3 client
    :param fixture_bucket_with_name: fixture to create a bucket with a unique name
    :param params: dict: 'file_path': str, 'file_size': dict, 'object_key': str
    :return: None
    """

    # Unpacking params
    file_path = params.get('file_path')
    download_path = file_path + "_downloaded"
    object_key = params.get('object_key')

    bucket_name = fixture_bucket_with_name
    total_size = create_big_file(file_path, params.get('file_size'))


    # Config for multhreading of boto3 building multipart upload/download
    config = TransferConfig(
        multipart_threshold=40 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )

    # Uploading the big file
    uploaded_file_size, _, _ = fixture_upload_multipart_file


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

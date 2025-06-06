import pytest
import tempfile
import os
import shutil
import logging

def verify_storage_class(s3_client, bucket_name, object_key, expected_storage_class):
    """Checks the storage class of an object, handling potential eventual consistency."""
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        actual_storage_class = response.get('StorageClass', 'STANDARD') # Default to STANDARD if not present
        logging.info(f"Head Object Response for {object_key}: {response}")

        if expected_storage_class in ('GLACIER_IR', 'COLD_INSTANT'):
            assert actual_storage_class in ('GLACIER_IR', 'COLD_INSTANT'), \
                f"Expected storage class GLACIER_IR or COLD_INSTANT, but got {actual_storage_class}"
        else:
            assert actual_storage_class == expected_storage_class, \
                f"Expected storage class {expected_storage_class}, but got {actual_storage_class}"
        logging.info(f"Successfully verified storage class for {object_key} as {actual_storage_class}")
        return
    except Exception as e:
        logging.error(f"Error during storage class verification for {object_key}: {e}")
        pytest.fail(f"Verification failed for {object_key} due to error: {e}")



@pytest.fixture
def create_multipart_object_files():
    object_key = "multipart_file.txt"
    body_size_mb = 20
    body = b"A" * (body_size_mb * 1024 * 1024) # 10 MB body

    # We'll create two parts based on the chunk size or just split in half
    part1_size = len(body) // 2
    part2_size = len(body) - part1_size
    parts_data = [body[:part1_size], body[part1_size:]]
    part_sizes = [part1_size, part2_size] # Store the actual sizes

    file_paths = []

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}") # For debugging

        for i, part_data in enumerate(parts_data):
            file_path = os.path.join(temp_dir, f"part_{i+1}.bin")
            with open(file_path, 'wb') as f:
                f.write(part_data)
            file_paths.append(file_path)
            print(f"Created temporary file: {file_path} with size {len(part_data)}") # For debugging

        yield object_key, file_paths, part_sizes, body # Also yield original body for verification if needed

    print(f"Temporary directory {temp_dir} and its contents have been removed.") # For debugging
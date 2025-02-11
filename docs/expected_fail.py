from utils.crud import (fixture_bucket_with_name,
                        list_all_objects,
                        upload_object,
                        download_object
                        )

def test_upload_download(s3_client, fixture_bucket_with_name):
    """
    Test the upload and download functionality of S3 client.
    
    Args:
        s3_client: The S3 client to use for the test.
        fixture_bucket_with_name: The name of the bucket fixture.
    """
    bucket = fixture_bucket_with_name
    
    upload_response = upload_object(s3_client, bucket, '123', '123')
    response = s3_client.get_object(Bucket=bucket, Key='123')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
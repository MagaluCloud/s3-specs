import pytest
from s3_specs.docs.tools.utils import generate_valid_bucket_name, fixture_create_small_file
from s3_specs.docs.tools.crud import create_bucket, upload_object, delete_bucket, delete_objects_multithreaded
from uuid import uuid4


@pytest.fixture
def fixture_versioned_bucket(s3_client, request):
    """
    Pytest fixture that creates an S3 bucket with versioning configuration.
    s3_client: Authenticated boto3 S3 client
    request: Pytest request object for parameter handling
    
    Yields: str: Name of the created bucket
    """
    try:
        # Get ACL from parameter or default to 'private'
        acl = getattr(request.param, 'acl', 'private')
        
        # Determine versioning status from parameter
        version_status = 'Enabled'
        
        # Generate unique bucket name from test name
        bucket_name = generate_valid_bucket_name(request.node.name.replace("_", "-"))
        
        # Create bucket and configure versioning
        create_bucket(s3_client, bucket_name, acl)
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': version_status}
        )
        
        yield bucket_name
        
    except Exception as e:
        pytest.fail(f"Fixture setup failed: {str(e)}")
        
    finally:
        # Cleanup - runs whether test passes or fails
        try:
            delete_objects_multithreaded(s3_client, bucket_name)
            delete_bucket(s3_client, bucket_name)
        except Exception as cleanup_error:
            pytest.fail(f"Fixture cleanup failed: {str(cleanup_error)}")

@pytest.fixture
def fixture_versioned_bucket_with_one_object(s3_client, fixture_versioned_bucket, fixture_create_small_file):

    # Local variables
    object_key = ("versioned_object_" + str(uuid4().hex))
    source_path = fixture_create_small_file
    # Calling fixture responsible to creating and teardown of the bucekt
    bucket_name = fixture_versioned_bucket

    try:
        upload_response = upload_object(
            s3_client,
            bucket_name=bucket_name,
            object_key=object_key,
            body_file=str(source_path)
        )
        # yield values 
        yield bucket_name, object_key, source_path
    except Exception as e:
        pytest.fail(f"Fixture setup failed: {e}")


    


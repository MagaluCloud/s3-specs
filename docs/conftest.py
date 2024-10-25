import boto3
import pytest

def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="default", help="AWS profile name")

@pytest.fixture
def s3_client(request):
    profile_name = request.config.getoption("--profile")
    session = boto3.Session(profile_name=profile_name)
    return session.client("s3")

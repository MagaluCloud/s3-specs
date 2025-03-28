import logging
import pytest

allow_get_object_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "<bucket_name>/*"
        }
    ]
}"""


# This fixture accepts indirect arguments in a dict:
#   policy_doc: is a string with a json and <bucket_name>
# on the places that will be replaced with the name
# of the generated bucket
#
# This fixture depends on the bucket_with_many_objects
# fixture so if your test needs custom object keys you
# need to indirect pass the arguments for that dependency
#
# This fixture depends on the multiple_s3_clients
# fixture so if your test needs custom number of clients you
# need to indirect pass the arguments for that dependency
@pytest.fixture(params=[{'policy_doc': allow_get_object_policy}])
def fixture_bucket_with_policy(request, multiple_s3_clients, bucket_with_many_objects):
    s3_clients = multiple_s3_clients
    bucket_owner_client = s3_clients[0]
    bucket_name, object_prefix, content = bucket_with_many_objects
    policy_template = request.param['policy_doc']
    policy_doc = policy_template.replace("<bucket_name>", bucket_name)
    bucket_owner_client.put_bucket_policy(Bucket=bucket_name, Policy = policy_doc)

    return bucket_name, policy_doc, s3_clients, object_prefix, content

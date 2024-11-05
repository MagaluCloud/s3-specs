# ---
# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # List buckets
#
# List all buckets from a profile[<sup>1</sup>](../glossary#profile)


# ## Examples

# + tags=["parameters"]
config = "../params/br-ne1.yaml"
docs_dir = "."
# -

# +
import random
import logging
from s3_helpers import run_example
# -

# +
def test_list_buckets(s3_client, profile_name="default"):
    response = s3_client.list_buckets()
    response_status = response["ResponseMetadata"]["HTTPStatusCode"]
    assert response_status == 200, "Expected HTTPStatusCode 200 for successful bucket list."
    buckets = response.get('Buckets')
    assert isinstance(buckets, list), "Expected 'Buckets' to be a list."
    buckets_count = len(buckets)
    assert isinstance(buckets_count, int), "Expected buckets count to be an integer."
    logging.info(f"Bucket list returned with status {response_status} and a list of {buckets_count} buckets")

    if buckets_count > 0:
        bucket_name = random.choice(buckets).get('Name')
        assert isinstance(bucket_name, str) and bucket_name, "Expected bucket name to be a non-empty string."
        logging.info(f"One of those buckets is named {random.choice(buckets).get('Name')}")

run_example(__name__, "list-buckets", "test_list_buckets", config=config, docs_dir=docs_dir)
# -


# ## References
#
# - [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)

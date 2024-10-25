# ---
# jupyter:
#   kernelspec:
#     name: my-poetry-env
#     display_name: Python 3
#   language_info:
#     name: python
# ---

# # List buckets
# List all buckets from a profile[<sup>1</sup>](./glossary#profile)


# + tags=["parameters"]
profile_name = "default"
# -


# + [markdown] jp-MarkdownHeadingCollapsed=true
# ## Setup


# +
import random

if __name__ != "__main__":
    import pytest
else:
    from s3_helpers import print_timestamp, create_s3_client
    print_timestamp()
    s3_client = create_s3_client(profile_name)
# -


# ## Example


# +
def test_list_buckets(s3_client, profile_name="default"):
    response = s3_client.list_buckets()
    buckets = response.get('Buckets')

    assert isinstance(buckets, list), "Expected 'Buckets' to be a list."
    buckets_count = len(buckets)
    assert isinstance(buckets_count, int), "Expected buckets count to be an integer."
    print(f"Profile '{profile_name}' has {buckets_count} buckets.")

    if buckets_count > 0:
        bucket_name = random.choice(buckets).get('Name')
        assert isinstance(bucket_name, str) and bucket_name, "Expected bucket name to be a non-empty string."
        print(f"One of those buckets is named {random.choice(buckets).get('Name')}")

if __name__ == "__main__":
    test_list_buckets(s3_client, profile_name)
# -


# ## References
#
# - [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)

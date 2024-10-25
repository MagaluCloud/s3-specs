# # List buckets
# List all buckets from a profile[<sup>1</sup>](./glossary#profile)


# + tags=["parameters"]
profile_name = "default"
# -


# + [markdown] jp-MarkdownHeadingCollapsed=true
# ## Setup


# +
# Import shared functions
from s3_helpers import print_timestamp, create_s3_client

print_timestamp()

# Create S3 client
s3_client = create_s3_client(profile_name)
# -


# ## Example


# +
response = s3_client.list_buckets()
buckets = response.get('Buckets')
buckets_count = len(buckets)
print(f"Profile '{profile_name}' has {buckets_count} buckets.")

if buckets_count > 0:
    import random
    print(f"One of those buckets is named {random.choice(buckets).get('Name')}")
# -


# ## References
#
# - [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)
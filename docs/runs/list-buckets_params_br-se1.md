# List buckets
List all buckets from a profile[<sup>1</sup>](./glossary#profile)

## Setup


```python
# Parameters
profile_name = "default"
```


```python
# Parameters
profile_name = "br-se1"

```


```python
import datetime
print(f'execution started at {datetime.datetime.now()}')
```

    execution started at 2024-10-25 10:10:36.457622



```python
# Client instantiation
import boto3
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3')
```

## Example


```python
response = s3_client.list_buckets()
buckets = response.get('Buckets')
print(f"Profile '{profile_name}' has {len(buckets)} buckets.")

import random
print(f"One of those buckets is named {random.choice(buckets).get('Name')}")
```

    Profile 'br-se1' has 4 buckets.
    One of those buckets is named benchmark


## References

- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html
  

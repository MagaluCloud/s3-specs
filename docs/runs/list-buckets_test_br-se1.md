# List buckets

Lista os buckets de um perfil[<sup>1</sup>](../glossary#profile)


```python
config = "../params/br-ne1.yaml"
docs_dir = "."
```


```python
# Parameters
config = "params/br-se1.yaml"
docs_dir = "docs"

```


```python
import random
import logging
import subprocess
import pytest
from shlex import split, quote
from s3_helpers import run_example
```

## Exemplos

### Boto3

O comando para listar buckets no boto3 é o `list_buckets`.


```python
def test_boto_list_buckets(s3_client, profile_name):
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

run_example(__name__, "list-buckets", "test_boto_list_buckets", config=config, docs_dir=docs_dir)
```

    
    docs/list-buckets_test.py::test_boto_list_buckets 

    
    -------------------------------- live log setup --------------------------------
    INFO     botocore.credentials:credentials.py:1278 Found credentials in shared credentials file: ~/.aws/credentials


    INFO     botocore.configprovider:configprovider.py:974 Found endpoint for s3 via: config_global.


    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:44 Bucket list returned with status 200 and a list of 8 buckets


    INFO     root:list-buckets_test.py:49 One of those buckets is named versioned-bucket-with-lock-0a302c


    PASSED

    
    
    ============================== 1 passed in 11.81s ==============================


### Rclone e AWS CLI

O comando para listar buckets no rclone é o `lsd`.
Os comandos para listar buckets na awscli são `s3 ls` e `s3api list-buckets`.

**Exemplos:**


```python
commands = [
    "rclone lsd {profile_name}:",
    "aws s3 ls --profile {profile_name}",
    "aws s3api list-buckets --profile {profile_name}",
]
```


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_cli_list_buckets(cmd_template, profile_name):
    cmd = split(cmd_template.format(profile_name=profile_name))
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "list-buckets", "test_cli_list_buckets", config=config, docs_dir=docs_dir)
```

    
    docs/list-buckets_test.py::test_cli_list_buckets[rclone lsd {profile_name}:] 

    
    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:73 Output from rclone lsd {profile_name}::           -1 2024-08-22 14:24:18        -1 benchmark
              -1 2024-10-03 15:01:42        -1 bug
              -1 2024-10-17 09:01:53        -1 test-201-1729166494-aws-br-se1
              -1 2024-11-06 16:00:15        -1 versioned-bucket-with-lock-0a302c
              -1 2024-11-06 16:01:28        -1 versioned-bucket-with-lock-0b4d12
              -1 2024-11-06 16:00:30        -1 versioned-bucket-with-lock-3ee101
              -1 2024-11-06 16:00:01        -1 versioned-bucket-with-lock-93da6b
              -1 2024-11-06 16:00:47        -1 versioned-bucket-with-lock-ee702e
    


    PASSED

    
    docs/list-buckets_test.py::test_cli_list_buckets[aws s3 ls --profile {profile_name}] 

    
    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:73 Output from aws s3 ls --profile {profile_name}: 2024-08-22 14:24:18 benchmark
    2024-10-03 15:01:42 bug
    2024-10-17 09:01:53 test-201-1729166494-aws-br-se1
    2024-11-06 16:00:15 versioned-bucket-with-lock-0a302c
    2024-11-06 16:01:28 versioned-bucket-with-lock-0b4d12
    2024-11-06 16:00:30 versioned-bucket-with-lock-3ee101
    2024-11-06 16:00:01 versioned-bucket-with-lock-93da6b
    2024-11-06 16:00:47 versioned-bucket-with-lock-ee702e
    


    PASSED

    
    docs/list-buckets_test.py::test_cli_list_buckets[aws s3api list-buckets --profile {profile_name}] 

    
    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:73 Output from aws s3api list-buckets --profile {profile_name}: {
        "Buckets": [
            {
                "Name": "benchmark",
                "CreationDate": "2024-08-22T17:24:18+00:00"
            },
            {
                "Name": "bug",
                "CreationDate": "2024-10-03T18:01:42+00:00"
            },
            {
                "Name": "test-201-1729166494-aws-br-se1",
                "CreationDate": "2024-10-17T12:01:53+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-0a302c",
                "CreationDate": "2024-11-06T19:00:15+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-0b4d12",
                "CreationDate": "2024-11-06T19:01:28+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-3ee101",
                "CreationDate": "2024-11-06T19:00:30+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-93da6b",
                "CreationDate": "2024-11-06T19:00:01+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-ee702e",
                "CreationDate": "2024-11-06T19:00:47+00:00"
            }
        ],
        "Owner": {
            "DisplayName": "a932e984-793c-48aa-bbf6-0434c9e4a12b",
            "ID": "a932e984-793c-48aa-bbf6-0434c9e4a12b"
        }
    }
    


    PASSED

    
    
    ============================== 3 passed in 36.84s ==============================


## Referências

- [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)
- [rclone lsd](https://rclone.org/commands/rclone_lsd/)
- [aws cli ls](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html)
- [aws cli list-buckets](https://docs.aws.amazon.com/cli/latest/reference/s3api/list-buckets.html)

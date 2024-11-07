# List buckets

Lista os buckets de um perfil[<sup>1</sup>](../glossary#profile)


```python
config = "../params/br-ne1.yaml"
docs_dir = "."
```


```python
# Parameters
config = "params/br-ne1.yaml"
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
    INFO     root:list-buckets_test.py:44 Bucket list returned with status 200 and a list of 31 buckets


    INFO     root:list-buckets_test.py:49 One of those buckets is named versioned-bucket-with-lock-7d23f9


    PASSED

    
    
    ============================== 1 passed in 1.51s ===============================


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
    INFO     root:list-buckets_test.py:73 Output from rclone lsd {profile_name}::           -1 2024-10-28 17:38:50        -1 s3-tester-e4b01a3c4a-foo
              -1 2024-10-26 06:40:40        -1 test-049-1729935638-mgc
              -1 2024-10-27 02:44:27        -1 test-049-1730007865-mgc
              -1 2024-10-29 07:53:50        -1 test-049-1730199229-mgc
              -1 2024-10-25 18:45:18        -1 test-br-ne1-1729892499
              -1 2024-10-26 01:49:23        -1 test-br-ne1-1729917937
              -1 2024-10-26 07:47:47        -1 test-br-ne1-1729939495
              -1 2024-10-26 20:40:17        -1 test-br-ne1-1729985832
              -1 2024-10-27 01:41:01        -1 test-br-ne1-1730004060555
              -1 2024-10-27 01:41:46        -1 test-br-ne1-1730004104351
              -1 2024-10-27 17:44:19        -1 test-br-ne1-1730061713
              -1 2024-10-28 11:48:16        -1 test-br-ne1-1730126677
              -1 2024-10-28 16:42:57        -1 test-br-ne1-1730144367
              -1 2024-10-29 00:51:20        -1 test-br-ne1-1730173655
              -1 2024-10-29 17:28:11        -1 versioned-bucket-with-lock-020ee8
              -1 2024-10-29 22:37:39        -1 versioned-bucket-with-lock-02317b
              -1 2024-11-06 14:52:18        -1 versioned-bucket-with-lock-14c824
              -1 2024-11-06 14:37:07        -1 versioned-bucket-with-lock-1feff0
              -1 2024-10-29 22:37:42        -1 versioned-bucket-with-lock-24e115
              -1 2024-10-29 17:28:06        -1 versioned-bucket-with-lock-2c2531
              -1 2024-11-06 14:52:32        -1 versioned-bucket-with-lock-4fdcf0
              -1 2024-10-29 20:08:06        -1 versioned-bucket-with-lock-548d31
              -1 2024-10-29 22:37:49        -1 versioned-bucket-with-lock-707c41
              -1 2024-10-29 20:08:13        -1 versioned-bucket-with-lock-7c0f93
              -1 2024-10-29 17:28:08        -1 versioned-bucket-with-lock-7d23f9
              -1 2024-10-29 20:08:03        -1 versioned-bucket-with-lock-97e68c
              -1 2024-10-29 17:28:04        -1 versioned-bucket-with-lock-c3596f
              -1 2024-11-06 14:52:49        -1 versioned-bucket-with-lock-d1cc65
              -1 2024-10-29 20:08:09        -1 versioned-bucket-with-lock-d6cf43
              -1 2024-10-29 22:37:46        -1 versioned-bucket-with-lock-ed67b6
              -1 2024-11-06 14:52:41        -1 versioned-bucket-with-lock-f9de84
    


    PASSED

    
    docs/list-buckets_test.py::test_cli_list_buckets[aws s3 ls --profile {profile_name}] 

    
    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:73 Output from aws s3 ls --profile {profile_name}: 2024-10-28 17:38:50 s3-tester-e4b01a3c4a-foo
    2024-10-26 06:40:40 test-049-1729935638-mgc
    2024-10-27 02:44:27 test-049-1730007865-mgc
    2024-10-29 07:53:50 test-049-1730199229-mgc
    2024-10-25 18:45:18 test-br-ne1-1729892499
    2024-10-26 01:49:23 test-br-ne1-1729917937
    2024-10-26 07:47:47 test-br-ne1-1729939495
    2024-10-26 20:40:17 test-br-ne1-1729985832
    2024-10-27 01:41:01 test-br-ne1-1730004060555
    2024-10-27 01:41:46 test-br-ne1-1730004104351
    2024-10-27 17:44:19 test-br-ne1-1730061713
    2024-10-28 11:48:16 test-br-ne1-1730126677
    2024-10-28 16:42:57 test-br-ne1-1730144367
    2024-10-29 00:51:20 test-br-ne1-1730173655
    2024-10-29 17:28:11 versioned-bucket-with-lock-020ee8
    2024-10-29 22:37:39 versioned-bucket-with-lock-02317b
    2024-11-06 14:52:18 versioned-bucket-with-lock-14c824
    2024-11-06 14:37:07 versioned-bucket-with-lock-1feff0
    2024-10-29 22:37:42 versioned-bucket-with-lock-24e115
    2024-10-29 17:28:06 versioned-bucket-with-lock-2c2531
    2024-11-06 14:52:32 versioned-bucket-with-lock-4fdcf0
    2024-10-29 20:08:06 versioned-bucket-with-lock-548d31
    2024-10-29 22:37:49 versioned-bucket-with-lock-707c41
    2024-10-29 20:08:13 versioned-bucket-with-lock-7c0f93
    2024-10-29 17:28:08 versioned-bucket-with-lock-7d23f9
    2024-10-29 20:08:03 versioned-bucket-with-lock-97e68c
    2024-10-29 17:28:04 versioned-bucket-with-lock-c3596f
    2024-11-06 14:52:49 versioned-bucket-with-lock-d1cc65
    2024-10-29 20:08:09 versioned-bucket-with-lock-d6cf43
    2024-10-29 22:37:46 versioned-bucket-with-lock-ed67b6
    2024-11-06 14:52:41 versioned-bucket-with-lock-f9de84
    


    PASSED

    
    docs/list-buckets_test.py::test_cli_list_buckets[aws s3api list-buckets --profile {profile_name}] 

    
    -------------------------------- live log call ---------------------------------
    INFO     root:list-buckets_test.py:73 Output from aws s3api list-buckets --profile {profile_name}: {
        "Buckets": [
            {
                "Name": "s3-tester-e4b01a3c4a-foo",
                "CreationDate": "2024-10-28T20:38:50+00:00"
            },
            {
                "Name": "test-049-1729935638-mgc",
                "CreationDate": "2024-10-26T09:40:40+00:00"
            },
            {
                "Name": "test-049-1730007865-mgc",
                "CreationDate": "2024-10-27T05:44:27+00:00"
            },
            {
                "Name": "test-049-1730199229-mgc",
                "CreationDate": "2024-10-29T10:53:50+00:00"
            },
            {
                "Name": "test-br-ne1-1729892499",
                "CreationDate": "2024-10-25T21:45:18+00:00"
            },
            {
                "Name": "test-br-ne1-1729917937",
                "CreationDate": "2024-10-26T04:49:23+00:00"
            },
            {
                "Name": "test-br-ne1-1729939495",
                "CreationDate": "2024-10-26T10:47:47+00:00"
            },
            {
                "Name": "test-br-ne1-1729985832",
                "CreationDate": "2024-10-26T23:40:17+00:00"
            },
            {
                "Name": "test-br-ne1-1730004060555",
                "CreationDate": "2024-10-27T04:41:01+00:00"
            },
            {
                "Name": "test-br-ne1-1730004104351",
                "CreationDate": "2024-10-27T04:41:46+00:00"
            },
            {
                "Name": "test-br-ne1-1730061713",
                "CreationDate": "2024-10-27T20:44:19+00:00"
            },
            {
                "Name": "test-br-ne1-1730126677",
                "CreationDate": "2024-10-28T14:48:16+00:00"
            },
            {
                "Name": "test-br-ne1-1730144367",
                "CreationDate": "2024-10-28T19:42:57+00:00"
            },
            {
                "Name": "test-br-ne1-1730173655",
                "CreationDate": "2024-10-29T03:51:20+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-020ee8",
                "CreationDate": "2024-10-29T20:28:11+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-02317b",
                "CreationDate": "2024-10-30T01:37:39+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-14c824",
                "CreationDate": "2024-11-06T17:52:18+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-1feff0",
                "CreationDate": "2024-11-06T17:37:07+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-24e115",
                "CreationDate": "2024-10-30T01:37:42+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-2c2531",
                "CreationDate": "2024-10-29T20:28:06+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-4fdcf0",
                "CreationDate": "2024-11-06T17:52:32+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-548d31",
                "CreationDate": "2024-10-29T23:08:06+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-707c41",
                "CreationDate": "2024-10-30T01:37:49+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-7c0f93",
                "CreationDate": "2024-10-29T23:08:13+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-7d23f9",
                "CreationDate": "2024-10-29T20:28:08+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-97e68c",
                "CreationDate": "2024-10-29T23:08:03+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-c3596f",
                "CreationDate": "2024-10-29T20:28:04+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-d1cc65",
                "CreationDate": "2024-11-06T17:52:49+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-d6cf43",
                "CreationDate": "2024-10-29T23:08:09+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-ed67b6",
                "CreationDate": "2024-10-30T01:37:46+00:00"
            },
            {
                "Name": "versioned-bucket-with-lock-f9de84",
                "CreationDate": "2024-11-06T17:52:41+00:00"
            }
        ],
        "Owner": {
            "DisplayName": "a932e984-793c-48aa-bbf6-0434c9e4a12b",
            "ID": "a932e984-793c-48aa-bbf6-0434c9e4a12b"
        }
    }
    


    PASSED

    
    
    ============================== 3 passed in 5.31s ===============================


## Referências

- [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)
- [rclone lsd](https://rclone.org/commands/rclone_lsd/)
- [aws cli ls](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html)
- [aws cli list-buckets](https://docs.aws.amazon.com/cli/latest/reference/s3api/list-buckets.html)

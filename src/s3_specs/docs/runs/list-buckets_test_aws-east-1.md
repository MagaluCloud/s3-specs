# List buckets

Lista os buckets de um perfil[<sup>1</sup>](../glossary#profile)


```python
config = "../params/br-ne1.yaml"
```


```python
import pytest
import random
import os
import logging
import subprocess
from shlex import split, quote
from s3_helpers import run_example
config = os.getenv("CONFIG", config)
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

run_example(__name__, "test_boto_list_buckets", config=config)
```

    .

                                                                            [100%]

    


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

run_example(__name__, "test_cli_list_buckets", config=config)
```

    .

    .

    .

                                                                          [100%]

    


## Referências

- [Boto3 Documentation: list_bucket](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html)
- [rclone lsd](https://rclone.org/commands/rclone_lsd/)
- [aws cli ls](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html)
- [aws cli list-buckets](https://docs.aws.amazon.com/cli/latest/reference/s3api/list-buckets.html)

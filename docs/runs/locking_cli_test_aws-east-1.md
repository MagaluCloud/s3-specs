# Uso de Object Locking via linha de comando

A funcionalidade de object locking permite a configuração de travas contra a remoção de objetos
por uma quantidades de dias determinada. Na MGC CLI, existem comandos para configurar estas travas
tanto a nivel de bucket quanto de object, bem como comandos para consultar estas configurações.


```python
config = "../params/aws-east-1.yaml"
```


```python
import boto3
import pytest
import logging
import subprocess
import json
import os
from shlex import split, quote
from s3_helpers import run_example, get_spec_path
from datetime import datetime, timedelta, timezone
```

## Exemplos

### Configurar uma trava padrão

Para setar uma regra de retenção padrão para todos os novos objetos escritos em um bucket, utilize
o comando `object-storage buckets object-lock set`, exemplos:


```python
commands = [
    "{mgc_path} object-storage buckets object-lock set {bucket_name} --days {days}",
    "{mgc_path} os buckets object-lock set --dst {bucket_name} --days {days}",
]
```


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_set_bucket_default_lock(cmd_template, active_mgc_workspace, mgc_path, lockeable_bucket_name):
    days = "1"
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=lockeable_bucket_name, days=days))

    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_set_bucket_default_lock", config=config)
```

    .

    .

                                                                           [100%]

    


### Consultar a configuração de locking em um bucket

Na mgc cli, o comando para consultar os parametros do locking padrão de um bucket é o
`buckets object-lock get`, exemplo:


```python
commands = [
    "{mgc_path} object-storage buckets object-lock get {bucket_name}",
]
```


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_get_bucket_default_lock(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock):
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_with_lock))
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_get_bucket_default_lock", config=config)
```

    .

                                                                            [100%]

    


### Configurar uma trava em apenas um objeto específico

Para setar uma regra de retenção a apenas um objeto em específico, utilize
o comando `object-storage objects object-lock set`, exemplos:


```python
commands = [
    "{mgc_path} object-storage objects object-lock set {bucket_name}/{object_key} --retain-until-date={retain_until_date}",
]
```


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_set_object_lock(cmd_template, active_mgc_workspace, mgc_path, bucket_with_one_object_and_lock_enabled, s3_client):
    # Set the retain-until date 24 hours from now
    retain_until_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")


    # Unpack bucket name, object key, and version from fixture
    bucket_name, object_key, object_version = bucket_with_one_object_and_lock_enabled

    # Format the CLI command
    cmd = split(cmd_template.format(
        mgc_path=mgc_path,
        bucket_name=bucket_name,
        object_key=object_key,
        retain_until_date=retain_until_date,
    ))

    # Run the CLI command
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Ensure the command executed successfully
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

    # Verify the object lock configuration using the s3_client fixture
    response = s3_client.head_object(Bucket=bucket_name, Key=object_key)

    # Ensure the object lock configuration matches the expected retain-until date
    assert 'ObjectLockRetainUntilDate' in response, "Object lock configuration not found in object metadata"
    returned_date = response['ObjectLockRetainUntilDate'].date().isoformat()
    assert returned_date == retain_until_date, (
        f"Expected retain-until-date {retain_until_date}, but got {returned_date.isoformat()}"
    )

    logging.info("Object lock configuration verified successfully.")

run_example(__name__, "test_set_bucket_default_lock", config=config)
```

    .

    .

                                                                           [100%]

    


### Consultar a trava de um objeto específico

Na mgc cli, o comando para consultar os parametros do locking de um bucket é o
`objects object-lock get`, exemplo:


```python
commands = [
    "{mgc_path} object-storage objects object-lock get {bucket_name}/{object_key}",
]
```


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_get_object_lock(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock_and_object):
    bucket_name, object_key, _ = bucket_with_lock_and_object
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd_template}: {result.stdout}")

run_example(__name__, "test_get_object_lock", config=config)
```

    .

                                                                            [100%]

    


### Soft delete vs permanent delete

Em um bucket com regra de object-lock, deletes simples ainda são permitidos,
por conta do bucket ser versionado estes não destroem dados, apenas adicionam
uma marca (delete marker). Já tentativas de deletar uma versão específica
de um objeto (permanent delete) são barradas pela trava.

#### Soft delete

Na mgc cli, um delete simples é feito com o comando `objects delete`, exemplo:


```python
commands = [
    "{mgc_path} object-storage objects delete {bucket_name}/{object_key} --no-confirm",
]
```

É esperado que o comando de delete comum (soft delete, sem version) retorne sucesso.


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_simple_delete_object_on_locked_bucket(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock_and_object):
    bucket_name, object_key, _ = bucket_with_lock_and_object

    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key))
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"
    logging.info(f"Output from {cmd}: {result.stdout}")

run_example(__name__, "test_simple_delete_object_on_locked_bucket", config=config)
```

    .

                                                                            [100%]

    


#### Permanent delete

Na mgc cli, um delete permanente é feito com o mesmo comando `objects delete`, porém incluindo
o parametro --obj-version para especificar qual versão de objeto deletar, exemplo:


```python
commands = [
    "{mgc_path} object-storage objects delete {bucket_name}/{object_key} --no-confirm --obj-version {object_version}",
]
```

É esperado que a versão do objeto continue presente mesmo após um comando de permanent delete (delete version)


```python
@pytest.mark.parametrize("cmd_template", commands)
def test_permanent_delete_object_on_locked_bucket(cmd_template, active_mgc_workspace, mgc_path, bucket_with_lock_and_object):
    bucket_name, object_key, object_version = bucket_with_lock_and_object

    cmd = split(f"{mgc_path} object-storage objects delete --no-confirm {bucket_name}/{object_key}  --obj-version {object_version}")
    cmd = split(cmd_template.format(mgc_path=mgc_path, bucket_name=bucket_name, object_key=object_key, object_version=object_version))
    result = subprocess.run(cmd, capture_output=True, text=True)
    # we do not assert the exit status of the command here because AWS may return a 200 with an AccessDenied xml inside and mgc cli will interpret it as success
    logging.info(f"Output from {cmd}: {result.stdout}")
    logging.info(f"Error from {cmd}: {result.stderr}")

    cmd_str = f"{mgc_path} object-storage objects versions {bucket_name}/{object_key} --raw "
    result = subprocess.run(split(cmd_str), capture_output=True, text=True)
    assert object_version in result.stdout, "Unexpected output: {result.stdout}"

run_example(__name__, "test_permanent_delete_object_on_locked_bucket", config=config)
```

    .

                                                                            [100%]

    


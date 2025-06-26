import pytest
import subprocess
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Função para contar objetos com um prefixo
def count_objects(profile_name, bucket_name, prefix):
    """
    Conta o número de objetos em um bucket S3 com um prefixo específico.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param prefix: Prefixo dos objetos a serem contados.
    :return: Número de objetos encontrados.
    """
    logging.info(
        f"Counting objects in bucket '{bucket_name}' with prefix '{prefix}' using profile '{profile_name}'"
    )
    result = subprocess.run(
        ['aws', '--profile', profile_name, 's3api', 'list-objects-v2',
        '--bucket', bucket_name, '--prefix', prefix,
        '--query', 'length(Contents || `[]`)'],
        capture_output=True, text=True
    )
    try:
        logging.warning(f"parsed {result.stdout.strip()}")
        return int(result.stdout.strip())
    except ValueError:
        logging.warning(f"Failed to parse count result: {result.stdout}")
        return 0

def check_count_objects(profile_name, bucket_name, prefix, expected_count):
    """
    Verifica se o número de objetos com o prefixo é igual ao esperado.
    """
    actual = count_objects(profile_name, bucket_name, prefix)
    logging.info(f"[count-objects] actual={actual}, expected={expected_count}")
    return actual == expected_count

# Função para criar objetos temporários
def create_temp_objects(quantity, size, dir_name):
    """
    Cria arquivos temporários com tamanho especificado em um diretório.
    :param quantity: Número de arquivos a serem criados.
    :param size: Tamanho de cada arquivo em KB.
    :param dir_name: Nome do diretório onde os arquivos serão criados.
    """
    logging.info(f"Creating {quantity} temporary files of size {size} KB in directory '{dir_name}'")
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    for i in range(1, quantity + 1):
        filename = os.path.join(dir_name, f"arquivo_{i}.txt")
        with open(filename, 'wb') as f:
            f.write(os.urandom(size * 1024))

# Função para fazer upload de objetos
def upload_objects(bucket_name, prefix, dir_name, workers):
    """
    Faz upload de objetos de um diretório para um bucket S3.
    :param bucket_name: Nome do bucket S3.
    :param prefix: Prefixo para os objetos no bucket.
    :param dir_name: Diretório contendo os arquivos a serem enviados.
    :param workers: Número de threads para upload paralelo.
    """
    logging.info(f"Uploading objects from directory '{dir_name}' to bucket '{bucket_name}' with {workers} workers")
    try:
        result = subprocess.run([
            'mgc', 'object-storage', 'objects', 'upload-dir', dir_name, f"{bucket_name}/{prefix}", '--workers', str(workers)
        ], check=True, capture_output=True, text=True)
        print("stdout:", result.stdout)
        print("stderr:", result.stderr)
    except subprocess.CalledProcessError as e:
        print("Erro ao executar o comando 'mgc':")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

# Funções para verificar a existência de objetos
def check_list_objects(profile_name, bucket_name, object_key):
    """
    Verifica se um objeto existe no bucket usando list-objects-v2.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param object_key: Chave do objeto a ser verificada.
    :return: True se o objeto existir, False caso contrário.
    """
    logging.info(
        f"Checking if object '{object_key}' exists in bucket '{bucket_name}' using profile '{profile_name}'"
    )
    result = subprocess.run(
        ['aws', '--profile', profile_name, 's3api', 'list-objects-v2',
         '--bucket', bucket_name, '--query', 'Contents[].Key'],
        capture_output=True, text=True
    )
    return object_key in result.stdout

# Função para verificar se um objeto pode ser recuperado
def check_get_object(profile_name, bucket_name, object_key):
    """
    Verifica se um objeto pode ser recuperado do bucket.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param object_key: Chave do objeto a ser verificada.
    :return: True se o objeto puder ser recuperado, False caso contrário.
    """
    logging.info(
        f"Checking if object '{object_key}' can be retrieved from bucket '{bucket_name}' using profile '{profile_name}'"
    )
    result = subprocess.run(
        ['aws', '--profile', profile_name, 's3api', 'get-object',
         '--bucket', bucket_name, '--key', object_key, '/dev/null'],
        capture_output=True
    )
    return result.returncode == 0

# Função para verificar se um objeto existe sem baixá-lo
def check_head_object(profile_name, bucket_name, object_key):
    """
    Verifica se um objeto existe no bucket usando head-object.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param object_key: Chave do objeto a ser verificada.
    :return: True se o objeto existir, False caso contrário.
    """
    logging.info(
        f"Checking if object '{object_key}' exists in bucket '{bucket_name}' using profile '{profile_name}'"
    )
    result = subprocess.run(
        ['aws', '--profile', profile_name, 's3api', 'head-object',
         '--bucket', bucket_name, '--key', object_key],
        capture_output=True
    )
    return result.returncode == 0

# Função para validar a existência de um objeto com múltiplas tentativas
def validate_key_with_n_successes(profile_name, bucket_name, object_key, required_successes, max_attempts, delay, expected_count=None):
    """
    Valida a existência de um objeto em um bucket S3 com múltiplas tentativas.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param object_key: Chave do objeto a ser verificada.
    :param required_successes: Número de sucessos necessários para considerar a validação bem-sucedida.
    :param max_attempts: Número máximo de tentativas.
    :param delay: Tempo de espera entre as tentativas em segundos.
    :param expected_count: Número esperado de objetos com o prefixo especificado (opcional).
    :return: Tupla contendo (sucesso, número de tentativas, timestamps, contadores de tentativas por comando).
    """
    logging.info(f"Validating key '{object_key}' in bucket '{bucket_name}' with {required_successes} required successes")
    counters = {
        "list-objects": 0,
        "get-object": 0,
        "head-object": 0,
        "count-objects": 0
    }
    timestamps = {name: None for name in counters}
    attempt_counters = {name: 0 for name in counters}

    check_funcs = {
        "list-objects": lambda: check_list_objects(profile_name, bucket_name, object_key),
        "get-object": lambda: check_get_object(profile_name, bucket_name, object_key),
        "head-object": lambda: check_head_object(profile_name, bucket_name, object_key),
        "count-objects": lambda: check_count_objects(profile_name, bucket_name, "additional", expected_count)
    }

    overall_start = time.time()

    for attempt in range(1, max_attempts + 1):
        logging.info(f"[{attempt}/{max_attempts}] Validating key '{object_key}' in bucket '{bucket_name}'")

        # Só roda os que ainda precisam de sucesso
        pending_checks = {name: check for name, check in check_funcs.items() if counters[name] < required_successes}

        if not pending_checks:
            logging.info(f"All checks reached {required_successes} successful validations.")
            return True, attempt, timestamps, attempt_counters

        with ThreadPoolExecutor(max_workers=len(pending_checks)) as executor:
            futures = {executor.submit(func): name for name, func in pending_checks.items()}

            for future in as_completed(futures):
                name = futures[future]
                attempt_counters[name] += 1
                try:
                    result = future.result()
                    if result:
                        counters[name] += 1
                        if counters[name] == required_successes:
                            timestamps[name] = time.time() - overall_start
                            logging.info(f"[{name}] reached {required_successes} successes at {timestamps[name]:.2f}s")
                except Exception as e:
                    logging.warning(f"[{name}] failed with exception: {e}")

        time.sleep(delay)

    logging.warning(f"Could not validate key '{object_key}' after {max_attempts} attempts.")
    return False, max_attempts, timestamps, attempt_counters

# Função para validar a ausência de um objeto com múltiplas tentativas
def validate_key_absent(profile_name, bucket_name, object_key, max_attempts, delay, required_successes=1):
    """
    Valida a ausência de um objeto em um bucket S3 com múltiplas tentativas.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param bucket_name: Nome do bucket S3.
    :param object_key: Chave do objeto a ser verificada.
    :param max_attempts: Número máximo de tentativas.
    :param delay: Tempo de espera entre as tentativas em segundos.
    :param required_successes: Número de sucessos necessários para considerar a validação bem-sucedida.
    :return: Tupla contendo (sucesso, número de tentativas, timestamps, contadores de tentativas por comando).
    """
    logging.info(f"Validating absence of key '{object_key}' in bucket '{bucket_name}' with {required_successes} required successes")
    counters = {
        "list-objects": 0,
        "get-object": 0,
        "head-object": 0,
        "count-objects": 0
    }
    timestamps = {name: None for name in counters}
    attempt_counters = {name: 0 for name in counters}

    check_funcs = {
        "list-objects": lambda: not check_list_objects(profile_name, bucket_name, object_key),
        "get-object": lambda: not check_get_object(profile_name, bucket_name, object_key),
        "head-object": lambda: not check_head_object(profile_name, bucket_name, object_key),
        "count-objects": lambda: check_count_objects(profile_name, bucket_name, "additional", 0)
    }

    overall_start = time.time()

    for attempt in range(1, max_attempts + 1):
        logging.info(f"[{attempt}/{max_attempts}] Checking if object '{object_key}' is gone from '{bucket_name}'")

        pending_checks = {name: func for name, func in check_funcs.items() if counters[name] < required_successes}

        if not pending_checks:
            return True, attempt, timestamps, attempt_counters

        with ThreadPoolExecutor(max_workers=len(pending_checks)) as executor:
            futures = {executor.submit(func): name for name, func in pending_checks.items()}

            for future in as_completed(futures):
                name = futures[future]
                attempt_counters[name] += 1
                try:
                    result = future.result()
                    if result:
                        counters[name] += 1
                        if counters[name] == required_successes:
                            timestamps[name] = time.time() - overall_start
                            logging.info(f"[{name}] absent success reached at {timestamps[name]:.2f}s")
                except Exception as e:
                    logging.warning(f"[{name}] failed with exception: {e}")

        time.sleep(delay)

    return False, max_attempts, timestamps, attempt_counters


# Fixture para criar o bucket e inicializá-lo com 10000 arquivos
@pytest.fixture(scope="session")
def setup_standard_bucket(session_active_mgc_workspace, session_bucket_with_one_object):
    """
    Cria um bucket padrão e inicializa com 10000 arquivos.
    :param session_active_mgc_workspace: Fixture que fornece o workspace ativo.
    :param session_bucket_with_one_object: Fixture que fornece um bucket com um objeto.
    :return: Nome do bucket criado.
    """
    logging.info("Setting up standard bucket with 10000 files")
    bucket_name, _, _ = session_bucket_with_one_object
    logging.info(f"Setting up standard bucket: {bucket_name}")

    # Cria o diretório temporário e os arquivos
    base_dir = "temp-report-10000-1"
    create_temp_objects(10000, 1, base_dir)

    upload_objects(bucket_name, "", base_dir, 256)

    return bucket_name

# Fixture para criar o bucket e inicializá-lo com 100 arquivos
@pytest.fixture(scope="session")
def setup_versioned_bucket(session_active_mgc_workspace, session_versioned_bucket_with_one_object):
    """
    Cria um bucket versionado e inicializa com 10000 arquivos.
    :param session_active_mgc_workspace: Fixture que fornece o workspace ativo.
    :param session_versioned_bucket_with_one_object: Fixture que fornece um bucket versionado com um objeto.
    :return: Nome do bucket criado.
    """
    logging.info("Setting up versioned bucket with 10000 files")
    bucket_name, _, _ = session_versioned_bucket_with_one_object
    logging.info(f"Setting up versioned bucket: {bucket_name}")

    base_dir = "temp-report-10000-1"
    create_temp_objects(1000, 1, base_dir)

    upload_objects(bucket_name, "", base_dir, 256)

    return bucket_name

# Fixture para resolver o bucket com base no tipo
@pytest.fixture
def resolve_bucket(request, bucket_type, setup_standard_bucket, setup_versioned_bucket):
    manual_standard = request.config.getoption("--manual-standard")
    manual_versioned = request.config.getoption("--manual-versioned")

    if bucket_type == "standard":
        return manual_standard if manual_standard else setup_standard_bucket
    elif bucket_type == "versioned":
        return manual_versioned if manual_versioned else setup_versioned_bucket
    else:
        raise ValueError(f"Unknown bucket_type: {bucket_type}")

# Fixture para fornecer os buckets disponíveis
@pytest.fixture(scope="session")
def available_buckets(
    request,
    setup_standard_bucket,
    setup_versioned_bucket,
):
    buckets = [
        ("auto-standard", setup_standard_bucket),
        ("auto-versioned", setup_versioned_bucket),
    ]

    manual_standard = request.config.getoption("--manual-standard")
    manual_versioned = request.config.getoption("--manual-versioned")

    if manual_standard:
        buckets.append(("manual-standard", manual_standard))
    if manual_versioned:
        buckets.append(("manual-versioned", manual_versioned))

    return buckets

# Testes de consistência para objetos adicionais
@pytest.mark.slow
@pytest.mark.multiple_objects
@pytest.mark.consistency
@pytest.mark.skip_if_dev
@pytest.mark.parametrize("quantity", [512])
@pytest.mark.parametrize("workers", [256])
def test_object_validations(available_buckets, profile_name, quantity, workers):
    """
    Testa a consistência de objetos adicionais em buckets S3.
    :param available_buckets: Lista de buckets disponíveis.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param quantity: Número de arquivos a serem criados.
    :param workers: Número de threads para upload paralelo.
    """
    logging.info(f"Starting consistency tests for {quantity} additional objects with {workers} workers")
    for bucket_type, bucket_name in available_buckets:
        prefix = "additional"
        temp_dir_for_additional_files = f"temp-report-{quantity}-additional"
        create_temp_objects(quantity, 1, temp_dir_for_additional_files)
        upload_objects(bucket_name, prefix, temp_dir_for_additional_files, workers)

        object_key = f"{prefix}/arquivo_{quantity}.txt"

        success, attempts, timestamps, attempts_per_command = validate_key_with_n_successes(
            profile_name,
            bucket_name,
            object_key,
            required_successes=3,
            max_attempts=10,
            delay=1,
            expected_count=quantity
        )

        assert success, f"Validation failed for key '{object_key}' after {attempts} attempts."

        with open('output/report_inconsistencies.csv', 'a') as f:
            for command in attempts_per_command:
                elapsed = timestamps.get(command, -1)
                f.write(f"{quantity},{workers},{command}_after_put,{profile_name},{bucket_type},{elapsed:.2f},{attempts_per_command[command]}\n")

# Testes de consistência para objetos deletados
@pytest.mark.slow
@pytest.mark.multiple_objects
@pytest.mark.consistency
@pytest.mark.skip_if_dev
@pytest.mark.parametrize("quantity", [512])
@pytest.mark.parametrize("workers", [256])
def test_deleted_object_validations(available_buckets, profile_name, quantity, workers):
    """
    Testa a remoção de objetos adicionais em buckets S3.
    :param available_buckets: Lista de buckets disponíveis.
    :param profile_name: Nome do perfil AWS a ser usado.
    :param quantity: Número de arquivos a serem criados.
    :param workers: Número de threads para upload paralelo.
    """
    logging.info(f"Starting deletion consistency tests for {quantity} additional objects with {workers} workers")
    for bucket_type, bucket_name in available_buckets:
        object_key = f"additional/arquivo_{quantity}.txt"

        logging.info(f"Removing all objects from bucket '{bucket_name}' using profile '{profile_name}'")
        subprocess.run([
            'aws', '--profile', profile_name, 's3', 'rm', f"s3://{bucket_name}/", '--recursive'
        ], check=True)

        logging.info(f"All objects removed from bucket '{bucket_name}'")

        success, attempts, timestamps, attempts_per_command = validate_key_absent(
            profile_name,
            bucket_name,
            object_key,
            required_successes=3,
            max_attempts=10,
            delay=1
        )
        
        assert success, f"Object '{object_key}' was expected to be deleted from bucket '{bucket_name}', but it was found."

        with open('output/report_inconsistencies.csv', 'a') as f:
            for command in attempts_per_command:
                elapsed = timestamps.get(command, -1)
                f.write(f"{quantity},{workers},{command}_after_delete,{profile_name},{bucket_type},{elapsed:.2f},{attempts_per_command[command]}\n")

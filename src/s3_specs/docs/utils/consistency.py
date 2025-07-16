import subprocess
import pytest
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

bucket_type_map = {
    "auto-standard": "1",
    "auto-versioned": "2",
    "manual-standard": "3",
    "manual-versioned": "4"
}

operation_map = {
    "put": "1",
    "delete": "2",
    "list": "3",
    "overwrite": "4"
}

command_map = {
    "head-object": "1",
    "get-object": "2",
    "list-objects": "3",
    "count-objects": "4"
}

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


# Fixture para criar o bucket e inicializá-lo com N arquivos
@pytest.fixture(scope="session")
def setup_standard_bucket(session_active_mgc_workspace, session_bucket_with_one_object):
    """
    Cria um bucket padrão e inicializa com N arquivos.
    :param session_active_mgc_workspace: Fixture que fornece o workspace ativo.
    :param session_bucket_with_one_object: Fixture que fornece um bucket com um objeto.
    :return: Nome do bucket criado.
    """
    bucket_name, _, _ = session_bucket_with_one_object
    n = 10000  # Número de arquivos a serem criados
    logging.info(f"Setting up standard bucket: {bucket_name} with {n} files")
    
    # Cria o diretório temporário e os arquivos
    base_dir = f"temp-report-{n}-1"
    create_temp_objects(n, 1, base_dir)

    upload_objects(bucket_name, "", base_dir, 256)

    return bucket_name

# Fixture para criar o bucket e inicializá-lo com N arquivos
@pytest.fixture(scope="session")
def setup_versioned_bucket(session_active_mgc_workspace, session_versioned_bucket_with_one_object):
    """
    Cria um bucket versionado e inicializa com N arquivos.
    :param session_active_mgc_workspace: Fixture que fornece o workspace ativo.
    :param session_versioned_bucket_with_one_object: Fixture que fornece um bucket versionado com um objeto.
    :return: Nome do bucket criado.
    """
    n = 10000  # Número de arquivos a serem criados
    bucket_name, _, _ = session_versioned_bucket_with_one_object
    logging.info(f"Setting up versioned bucket: {bucket_name} with {n} files")

    base_dir = f"temp-report-{n}-1"
    create_temp_objects(n, 1, base_dir)

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

# Função para verificar se o número de objetos com um prefixo é igual ao esperado
def check_count_objects(profile_name, bucket_name, prefix, expected_count):
    """
    Verifica se o número de objetos com o prefixo é igual ao esperado.
    """
    actual = count_objects(profile_name, bucket_name, prefix)
    logging.info(f"[count-objects] actual={actual}, expected={expected_count}")
    return actual == expected_count

# Função para sobrescrever e validar leituras consistentes
def overwrite_and_validate_reads(bucket_name, object_key, quantity, profile_name, bucket_type, read_repeats=3, delay=1):
    """
    Realiza sobrescritas e valida leituras consistentes após cada uma.
    :param bucket_name: Nome do bucket.
    :param object_key: Chave do objeto.
    :param quantity: Número de sobrescritas a realizar.
    :param profile_name: Perfil AWS.
    :param bucket_type: Tipo de bucket (para logs/CSV).
    :param read_repeats: Número de leituras consistentes por sobrescrita.
    :param delay: Tempo de espera entre leitura após escrita.
    """
    temp_filename = "/tmp/overwrite_temp.txt"
    temp_download = "/tmp/overwrite_download.txt"
    os.makedirs("output", exist_ok=True)

    total_success = True  # Assume que todas sobrescritas serão consistentes

    for i in range(1, quantity + 1):
        content = f"conteudo_sobrescrito_{i}"
        logging.info(f"[{bucket_type}] Iteração {i}: Iniciando sobrescrita com conteúdo: '{content}'")

        with open(temp_filename, "w") as f:
            f.write(content)

        start = time.time()

        # Escrita no bucket
        subprocess.run([
            'aws', '--profile', profile_name, 's3', 'cp', temp_filename,
            f"s3://{bucket_name}/{object_key}"
        ], check=True)
        logging.info(f"[{bucket_type}] Iteração {i}: Escrita concluída em s3://{bucket_name}/{object_key}")

        time.sleep(delay)

        success_reads = 0
        for attempt in range(1, read_repeats + 1):
            logging.info(f"[{bucket_type}] Iteração {i}, leitura {attempt}: Iniciando leitura")
            subprocess.run([
                'aws', '--profile', profile_name, 's3', 'cp',
                f"s3://{bucket_name}/{object_key}", temp_download
            ], check=True)

            with open(temp_download, "r") as f:
                actual_content = f.read()

            if actual_content == content:
                logging.info(f"[{bucket_type}] Iteração {i}, leitura {attempt}: ✅ Conteúdo consistente.")
                success_reads += 1
            else:
                logging.warning(
                    f"[{bucket_type}] Iteração {i}, leitura {attempt}: ❌ Inconsistência detectada.\n"
                    f"  → Esperado: '{content}'\n"
                    f"  → Encontrado: '{actual_content}'"
                )

            time.sleep(delay)

        if success_reads != read_repeats:
            logging.error(
                f"[{bucket_type}] Iteração {i}: Apenas {success_reads}/{read_repeats} leituras foram consistentes."
            )
            total_success = False
        else:
            logging.info(f"[{bucket_type}] Iteração {i}: Todas as {read_repeats} leituras foram consistentes.")

    elapsed = time.time() - start

    if total_success:
        logging.info(f"[{bucket_type}] Todas as {quantity} sobrescritas foram validadas com sucesso.")
        with open("output/report_inconsistencies.csv", "a") as f:
            f.write(f"{quantity},1,overwrite_{quantity}_times,{profile_name},{bucket_type},{elapsed:.2f},{read_repeats}\n")
            bucket_id = bucket_type_map.get(bucket_type, bucket_type)
            operation_id = operation_map["overwrite"]
            f.write(f"{quantity},1,{operation_id}_{quantity},{profile_name},{bucket_id},{elapsed:.2f},{read_repeats}\n")
    else:
        logging.error(f"[{bucket_type}] Falha de consistência em uma ou mais sobrescritas.")
        pytest.fail(f"[{bucket_type}] Nem todas as sobrescritas foram consistentes após {read_repeats} leituras.")

import pytest
import subprocess
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Função para contar objetos com um prefixo
def count_objects(profile_name, bucket_name, prefix):
    def list_objects_with_prefix():
        result = subprocess.run(
            ['aws', '--profile', profile_name, 's3api', 'list-objects-v2', '--bucket', bucket_name, '--prefix', prefix, '--query', 'length(Contents)'],
            capture_output=True, text=True
        )
        try:
            return int(result.stdout.strip())  # Retorna o número de objetos com o prefixo
        except ValueError:
            print(f"Error parsing the count output: {result.stdout}")
            return 0

    success_count = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(list_objects_with_prefix) for _ in range(10)]  # Envia 10 verificações

        for future in as_completed(futures):
            result = future.result()
            if result > 0:
                success_count += 1

            # Se tivermos 10 verificações bem-sucedidas, retornamos o valor
            if success_count >= 10:
                return result

    return 0

# Função para criar objetos temporários
def create_temp_objects(quantity, size, dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    for i in range(1, quantity + 1):
        filename = os.path.join(dir_name, f"arquivo_{i}.txt")
        with open(filename, 'wb') as f:
            f.write(os.urandom(size * 1024))

# Função para fazer upload de objetos
def upload_objects(bucket_name, dir_name, workers):
    try:
        result = subprocess.run([
            'mgc', 'object-storage', 'objects', 'upload-dir', dir_name, f"{bucket_name}/", '--workers', str(workers)
        ], check=True, capture_output=True, text=True)
        print("stdout:", result.stdout)
        print("stderr:", result.stderr)
    except subprocess.CalledProcessError as e:
        print("Erro ao executar o comando 'mgc':")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

# Função de validação de objeto
def validate_key_in_objects(command, profile_name, bucket_name, object_key, required_successes):
    success_count = 0
    start_time = time.time()

    def check_key_in_object(command, profile_name, bucket_name, object_key):
        if command == 'list-objects':
            result = subprocess.run(
                ['aws', '--profile', profile_name, 's3api', 'list-objects-v2', '--bucket', bucket_name, '--query', 'Contents[].Key'],
                capture_output=True, text=True
            )
            objects = result.stdout.split()
            return object_key in objects
        elif command == 'get-object':
            result = subprocess.run(
                ['aws', '--profile', profile_name, 's3api', 'get-object', '--bucket', bucket_name, '--key', object_key, '/dev/null'],
                capture_output=True
            )
            return result.returncode == 0
        elif command == 'head-object':
            result = subprocess.run(
                ['aws', '--profile', profile_name, 's3api', 'head-object', '--bucket', bucket_name, '--key', object_key],
                capture_output=True
            )
            return result.returncode == 0
        return False

    with ThreadPoolExecutor(max_workers=required_successes) as executor:
        futures = [executor.submit(check_key_in_object, command, profile_name, bucket_name, object_key) for _ in range(required_successes)]

        for future in as_completed(futures):
            if future.result():
                success_count += 1

            if success_count >= required_successes:
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"All {required_successes} checks passed successfully. Total time: {elapsed_time:.2f} seconds.")
                return True

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Max retries reached or not enough successful checks. Total time: {elapsed_time:.2f} seconds.")
        return False

# Fixture para criar o bucket e inicializá-lo com 100000 arquivos
@pytest.fixture(scope="session")
def setup_standard_bucket(session_active_mgc_workspace, session_bucket_with_one_object):
    bucket_name, _, _ = session_bucket_with_one_object

    # Cria o diretório temporário e os arquivos
    base_dir = "temp-report-100000-1"
    create_temp_objects(100000, 1, base_dir)

    upload_objects(bucket_name, base_dir, 256)

    return bucket_name

# Fixture para criar o bucket e inicializá-lo com 100 arquivos
@pytest.fixture(scope="session")
def setup_versioned_bucket(session_active_mgc_workspace, session_versioned_bucket_with_one_object):
    bucket_name, _, _ = session_versioned_bucket_with_one_object

    base_dir = "temp-report-100000-1"
    create_temp_objects(100000, 1, base_dir)

    upload_objects(bucket_name, base_dir, 256)

    return bucket_name

# Teste principal
@pytest.mark.slow
@pytest.mark.multiple_objects
@pytest.mark.consistency
@pytest.mark.parametrize("command", ["get-object", "list-objects", "head-object", "count-objects"])
@pytest.mark.parametrize("quantity", [512])  # Quantidade de arquivos a serem adicionados
@pytest.mark.parametrize("workers", [256])  # Número de workers para o upload
@pytest.mark.parametrize("bucket_type", ["standard", "versioned"])  # Parametriza entre bucket normal e versionado
def test_object_validations(setup_standard_bucket, setup_versioned_bucket, command, profile_name, quantity, workers, bucket_type):
    if bucket_type == "versioned":
        bucket_name = setup_versioned_bucket
    else:
        bucket_name = setup_standard_bucket

    # Prefixo para os uploads
    prefix = f"{command}/arquivo_"

    # Cria arquivos temporários e realiza upload
    temp_dir_for_additional_files = f"temp-report-{quantity}-additional"
    create_temp_objects(quantity, 1, temp_dir_for_additional_files)
    upload_objects(bucket_name, temp_dir_for_additional_files, workers)

    start_time = time.time()

    if command == "count-objects":
        actual_count = count_objects(profile_name, bucket_name, prefix)
        if actual_count == quantity:
            print(f"Correct count of objects with prefix '{prefix}': {actual_count} objects.")
        else:
            print(f"Count mismatch: Expected {quantity} objects, but found {actual_count} objects.")
    else:
        object_key = f"{prefix}{quantity}.txt"
        required_successes = 10
        if validate_key_in_objects(command, profile_name, bucket_name, object_key, required_successes):
            print(f"Key '{object_key}' found.")
        else:
            print(f"Key '{object_key}' not found.")

    end_time = time.time()
    elapsed_time = end_time - start_time

    # Salva os resultados
    with open('output/report_inconsistencies.csv', 'a') as f:
        f.write(f"{quantity},{workers},{command}_after_put,{profile_name},{bucket_type},{elapsed_time}\n")

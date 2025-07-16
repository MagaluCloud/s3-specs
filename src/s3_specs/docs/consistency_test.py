import pytest
import subprocess
import logging
from s3_specs.docs.utils.consistency import (
    create_temp_objects,
    upload_objects,
    validate_key_with_n_successes,
    validate_key_absent,
    bucket_type_map,
    command_map,
    operation_map
)

# Testes de consistência para objetos adicionais
@pytest.mark.slow
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
                bucket_id = bucket_type_map.get(bucket_type, bucket_type)
                command_id = command_map.get(command, "0")
                operation_id = operation_map["put"]

                f.write(f"{quantity},{workers},{operation_id}_{command_id},{profile_name},{bucket_id},{elapsed:.2f},{attempts_per_command[command]}\n")

# Testes de consistência para objetos deletados
@pytest.mark.slow
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
                bucket_id = bucket_type_map.get(bucket_type, bucket_type)
                command_id = command_map.get(command, "0")
                operation_id = operation_map["delete"]

                f.write(f"{quantity},{workers},{operation_id}_{command_id},{profile_name},{bucket_id},{elapsed:.2f},{attempts_per_command[command]}\n")

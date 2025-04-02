from datetime import datetime
import os
import subprocess
import pytest
import tempfile

# Função para medir o tempo de uma operação em millisegundos
def measure_time(command):
    t0 = datetime.now()
    subprocess.run(command, check=True, shell=True, stdout=subprocess.DEVNULL)
    return int(1000 * (datetime.now() - t0).total_seconds())


# Lista de comandos a serem parametrizados
commands = [
    [
        "aws --profile {profile_name} s3 cp {temp_dir} s3://{bucket_name}/{prefix}/ --recursive",
        "aws --profile {profile_name} s3 cp s3://{bucket_name}/{prefix}/ ./temp-down-{prefix}-aws --recursive",
        "aws --profile {profile_name} s3 rm s3://{bucket_name}/{prefix}/ --recursive",
    ],
    [
        "rclone copy {temp_dir} {profile_name}:{bucket_name}/{prefix}/ --transfers={workers}",
        "rclone copy {profile_name}:{bucket_name}/{prefix}/ ./temp-down-{prefix}-rclone --transfers={workers}",
        "rclone delete {profile_name}:{bucket_name}/{prefix}/",
    ],
    [
        "mgc object-storage objects upload-dir {temp_dir} {bucket_name}/{prefix}/ --workers {workers}",
        "mgc object-storage objects download-all {bucket_name}/{prefix}/ ./temp-down-{prefix}-mgc",
        "mgc object-storage objects delete-all {bucket_name}/{prefix}/ --no-confirm",
    ]
]


# Parametrização dos parâmetros
@pytest.mark.parametrize(
    "cmd_templates",
    commands
)
@pytest.mark.parametrize(
    "sizes, quantity, workers, times",
    [
        ([1, 5000, 9000], 1, 4, 10),
        ([1, 5000, 9000], 30, 4, 10),
        ([1, 5000, 9000], 60, 4, 10),
        ([1, 5000, 9000], 90, 4, 10),
    ]
)
@pytest.mark.benchmark
def test_benchmark(
    session_bucket_with_one_object,
    cmd_templates,
    profile_name,
    sizes,
    quantity,
    workers,
    times
):
    """Testa os benchmarks com os parâmetros fornecidos"""

    # Obtenha o nome do bucket e a chave do objeto
    bucket_name, obj_key, _ = session_bucket_with_one_object

    for size in sizes:  # Agora, iterando sobre os diferentes tamanhos
        with tempfile.TemporaryDirectory() as temp_dir:
            # Cria o diretório temporário para os arquivos de teste (simulando arquivos a serem carregados)
            os.makedirs(temp_dir, exist_ok=True)

            # Criação de arquivos temporários para upload
            for i in range(quantity):
                file_name = f"{temp_dir}/file_{size}_{i}.txt"  # Nome único para cada arquivo

                # Criando um arquivo temporário com o tamanho especificado
                with open(file_name, 'wb') as f:
                    f.write(b"0" * (int(size) * 1024))  # Escreve o conteúdo do arquivo com o tamanho especificado (em KB)

            for cmd_template in cmd_templates:
                # Substitua os parâmetros no comando, incluindo o arquivo correto para a operação
                for i in range(times):  # Agora, iterando sobre o número de vezes para rodar os testes
                    # Criar o prefixo para essa iteração
                    prefix = f"{size}-{quantity}-{i}"  # Prefixo único por iteração

                    for j in range(quantity):  # Para cada arquivo
                        cmd = cmd_template.format(
                            profile_name=profile_name,
                            temp_dir=temp_dir,
                            bucket_name=bucket_name,
                            workers=workers,
                            prefix=prefix  # Incluindo o prefixo
                        )

                        # Verificar se a pasta report existe, senão cria
                        os.makedirs("report", exist_ok=True)

                        # Caminho do arquivo de relatório
                        report_file = "output/benchmark_results.csv"

                        time_taken = measure_time(cmd)
                        subprocess.run(f"rm -rf temp-down-*", shell=True)
                        # Salva os resultados no arquivo de relatório
                        with open(report_file, "a") as f:
                            tool = cmd.split()[0]
                            operation = (
                                "upload" if ("cp" in cmd and "./" not in cmd) or ("copy" in cmd and "./" not in cmd) or "upload-dir" in cmd else
                                "download" if ("cp" in cmd and "./" in cmd) or ("copy" in cmd and "./" in cmd) or "download-all" in cmd else
                                "delete" if "rm" in cmd or "delete-all" in cmd or "delete" in cmd else
                                "unknown"
                            )
                            f.write(f"{profile_name},{tool},{size},{times},{workers},{quantity},{operation},{time_taken}\n")

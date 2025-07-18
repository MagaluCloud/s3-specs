import os
import time
import json
import subprocess
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def cleanup_prefix(bucket_name, prefix, profile, retry_count, retry_delay):
    """
    Limpa o prefixo especificado no bucket S3.
    Tenta várias vezes antes de falhar permanentemente.
    """
    print(f"Limpando bucket s3://{bucket_name}/{prefix} antes de iniciar...")
    for attempt in range(retry_count):
        result = subprocess.run(
            ["aws", "--profile", profile, "s3", "rm", f"s3://{bucket_name}/{prefix}", "--recursive"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"Cleanup em {bucket_name} concluído.")
            return
        else:
            print(f"Falha ao limpar prefixo em {bucket_name}, tentativa {attempt + 1}/{retry_count}")
            time.sleep(retry_delay)
    print(f"Erro permanente ao limpar {bucket_name}:\n{result.stderr}")


def upload_object(index, bucket_name, prefix, profile, retry_count, retry_delay):
    """
    Faz upload de um objeto para o bucket S3 especificado.
    Cria um arquivo temporário com conteúdo baseado no índice e faz o upload.
    """
    key = f"{prefix}obj_{index}.txt"
    path = f"/tmp/{bucket_name.replace('/', '_')}_{key}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(f"Conteúdo {index} - {datetime.now()}")

    for attempt in range(retry_count):
        result = subprocess.run([
            "aws", "--profile", profile, "s3", "cp", path, f"s3://{bucket_name}/{key}"
        ])
        if result.returncode == 0:
            print(f"Upload OK ({bucket_name}): {key}")
            return key
        else:
            print(f"Erro no upload ({bucket_name}, {key}), tentativa {attempt + 1}/{retry_count}")
            time.sleep(retry_delay)
    raise Exception(f"Falha ao fazer upload: {key}")


def delete_object(key, bucket_name, profile, retry_count, retry_delay):
    """
    Deleta um objeto do bucket S3 especificado.
    Tenta várias vezes antes de falhar permanentemente.
    """
    for attempt in range(retry_count):
        result = subprocess.run([
            "aws", "--profile", profile, "s3", "rm", f"s3://{bucket_name}/{key}"
        ])
        if result.returncode == 0:
            print(f"Deletado ({bucket_name}): {key}")
            return
        else:
            print(f"Erro ao deletar {key} ({bucket_name}), tentativa {attempt + 1}/{retry_count}")
            time.sleep(retry_delay)
    print(f"Falha ao deletar permanentemente: {key} ({bucket_name})")


def list_objects(bucket_name, prefix, profile, retry_count, retry_delay):
    """
    Lista objetos no bucket S3 especificado com o prefixo dado.
    Retorna uma lista de chaves dos objetos encontrados.
    Tenta várias vezes antes de falhar permanentemente.
    """
    for attempt in range(retry_count):
        result = subprocess.run(
            [
                "aws", "--profile", profile, "s3api", "list-objects-v2",
                "--bucket", bucket_name, "--prefix", prefix,
                "--query", "Contents[].Key", "--output", "json"
            ],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                print(f"Erro ao parsear JSON da listagem em {bucket_name}")
        else:
            print(f"Erro ao listar objetos em {bucket_name}, tentativa {attempt + 1}/{retry_count}")
        time.sleep(retry_delay)
    return []


def upload_batch(start_index, batch_size, bucket_name, prefix, profile, retry_count, retry_delay, max_workers):
    """
    Faz upload de um lote de objetos para o bucket S3 especificado.
    Utiliza ThreadPoolExecutor para uploads paralelos.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_object, i, bucket_name, prefix, profile, retry_count, retry_delay)
                   for i in range(start_index, start_index + batch_size)]
        return [future.result() for future in as_completed(futures)]


def write_csv(timestamp, bucket_name, esperados, encontrados, debug_csv, csv_path):
    """
    Escreve os resultados em um arquivo CSV.
    Compara os objetos esperados com os encontrados e registra inconsistências.
    """
    esperados_set = set(esperados)
    encontrados_set = set(encontrados)
    faltando = esperados_set - encontrados_set
    extras = encontrados_set - esperados_set

    if faltando or extras or debug_csv:
        with open(csv_path, "a") as f:
            f.write(
                f"{timestamp},{bucket_name},{len(esperados_set)},{len(encontrados_set)},"
                f"{len(faltando)},{len(extras)}\n"
            )
        if faltando or extras:
            print(f"Inconsistência em {bucket_name}: {len(faltando)} faltando, {len(extras)} a mais.")
    else:
        print(f"Todos os objetos esperados foram listados em {bucket_name}.")


def loop_monitor(bucket_name, bucket_id, args):
    """
    Loop principal que monitora o bucket S3 especificado.
    Faz upload de objetos, remove o mais antigo e verifica a consistência.
    """
    index = 0
    objetos_ativos = []

    cleanup_prefix(bucket_name, args.prefix, args.profile, args.retry_count, args.retry_delay)

    print(f"Inicializando bucket {bucket_name} com {args.object_limit} objetos.")
    objetos_ativos.extend(upload_batch(
        index, args.object_limit, bucket_name, args.prefix, args.profile,
        args.retry_count, args.retry_delay, args.max_workers
    ))
    index += args.object_limit

    while True:
        timestamp = datetime.utcnow().timestamp()
        novos = upload_batch(index, 1, bucket_name, args.prefix, args.profile,
                             args.retry_count, args.retry_delay, args.max_workers)
        objetos_ativos.extend(novos)
        index += 1

        key_remover = objetos_ativos.pop(0)
        delete_object(key_remover, bucket_name, args.profile, args.retry_count, args.retry_delay)

        lista_real = list_objects(bucket_name, args.prefix, args.profile, args.retry_count, args.retry_delay)
        write_csv(timestamp, bucket_id, objetos_ativos, lista_real, args.debug, args.csv_path)

        print(f"Loop completo ({bucket_name}). Esperados: {len(objetos_ativos)}, Encontrados: {len(lista_real)}")
        time.sleep(2)


def main():
    """
    Função principal que configura o monitoramento de consistência S3 rotativo.
    Lê os argumentos da linha de comando e inicia o loop de monitoramento.
    """
    parser = argparse.ArgumentParser(description="Monitor de consistência S3 rotativo")
    parser.add_argument("--buckets", required=True, help="Lista de buckets separados por vírgula")
    parser.add_argument("--object-limit", type=int, default=1000, help="Número de objetos para iniciar")
    parser.add_argument("--prefix", default="rotativo-test/", help="Prefixo dos objetos")
    parser.add_argument("--profile", default="se1", help="AWS profile")
    parser.add_argument("--max-workers", type=int, default=100, help="Threads para uploads simultâneos")
    parser.add_argument("--threads-por-bucket", type=int, default=1, help="Threads por bucket")
    parser.add_argument("--retry-count", type=int, default=3, help="Tentativas de retry")
    parser.add_argument("--retry-delay", type=int, default=0, help="Delay entre tentativas (segundos)")
    parser.add_argument("--csv-path", default="./output/rotativo_metrics.csv", help="Arquivo CSV de saída")
    parser.add_argument("--debug", action="store_true", help="Escreve CSV mesmo sem inconsistência")
    args = parser.parse_args()

    bucket_list = [b.strip() for b in args.buckets.split(",") if b.strip()]

    if not bucket_list:
        print("Nenhum bucket informado.")
        return

    if not os.path.exists(args.csv_path):
        with open(args.csv_path, "w") as f:
            f.write("timestamp,bucket,expected,found,missing,unexpected\n")

    bucket_id_map = {bucket: i + 1 for i, bucket in enumerate(bucket_list)}

    with ThreadPoolExecutor(max_workers=len(bucket_list) * args.threads_por_bucket) as executor:
        for bucket in bucket_list:
            bucket_id = bucket_id_map[bucket]
            executor.submit(loop_monitor, bucket, bucket_id, args)

if __name__ == "__main__":
    main()

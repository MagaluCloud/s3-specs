import argparse
from datetime import datetime, timezone
import os
import subprocess
import tempfile

commands = [
    "mgc object-storage objects upload-dir {temp_dir} {bucket_name}/{prefix}/ --workers {workers}",
    "mgc object-storage objects download-all {bucket_name}/{prefix}/ ./temp-down-{prefix}-mgc --workers {workers}",
    "mgc object-storage objects delete-all {bucket_name}/{prefix}/ --no-confirm --workers {workers}",
    "mgc object-storage objects list {bucket_name} --max-items 999999999"
]

def measure_time(command):
    t0 = datetime.now()
    try:
        subprocess.run(command, check=True, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar: {command}")
        raise
    return int(1000 * (datetime.now() - t0).total_seconds())

def generate_files(temp_dir, size_kb, quantity):
    for i in range(quantity):
        file_name = f"{temp_dir}/file_{size_kb}_{i}.txt"
        with open(file_name, 'wb') as f:
            f.write(b"0" * (size_kb * 1024))

def main():
    parser = argparse.ArgumentParser(description="Benchmark S3 mgc-cli em loop infinito.")
    parser.add_argument("--buckets", required=True, help="Buckets separados por vírgula para todos os testes exceto list")
    parser.add_argument("--list-buckets", help="Buckets separados por vírgula para o teste de list")
    parser.add_argument("--profile", default="default", help="Nome do perfil AWS")
    parser.add_argument("--sizes", default="1", help="Tamanhos dos arquivos em KB separados por vírgula, ex: 1,10,100")
    parser.add_argument("--quantity", type=int, default=10000, help="Quantidade de arquivos por fase")
    parser.add_argument("--workers", type=int, default=256, help="Número de workers paralelos")
    parser.add_argument("--times", type=int, default=1, help="Número de repetições por comando")
    parser.add_argument("--output", default="output/new_benchmark_results.csv", help="Caminho do CSV para salvar resultados")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",")]
    list_buckets = args.list_buckets.split(",") if args.list_buckets else []
    test_buckets = args.buckets.split(",")
    bucket_id_map = {name: str(i + 1) for i, name in enumerate(set(test_buckets + list_buckets))}
    operation_id_map = {
        "upload": "1",
        "download": "2",
        "delete": "3",
        "list": "4"
    }
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    if not os.path.exists(args.output):
        with open(args.output, "w") as f:
            f.write("timestamp,region,operation,bucket,size,quantity,workers,duration_ms,tps,success\n")

    try:
        while True:
            for size in sizes:
                with tempfile.TemporaryDirectory() as temp_dir:
                    os.makedirs(temp_dir, exist_ok=True)
                    print(f"Gerando {args.quantity} arquivos de {size}KB em {temp_dir}...")
                    generate_files(temp_dir, size, args.quantity)

                    for i in range(args.times):
                        prefix = f"{size}-{args.quantity}-{i}"

                        for cmd_template in commands:
                            is_list = "list" in cmd_template
                            buckets_to_use = list_buckets if is_list else test_buckets

                            for bucket_name in buckets_to_use:
                                if "download-all" in cmd_template:
                                    download_dir = f"./temp-down-{prefix}-mgc"
                                    os.makedirs(download_dir, exist_ok=True)

                                cmd = cmd_template.format(
                                    profile_name=args.profile,
                                    temp_dir=temp_dir,
                                    bucket_name=bucket_name,
                                    workers=args.workers,
                                    prefix=prefix
                                )

                                success = 1
                                print(f"Executando: {cmd}")
                                try:
                                    duration_ms = measure_time(cmd)
                                except Exception as e:
                                    success = 0
                                    duration_ms = -1

                                subprocess.run("rm -rf temp-down-*", shell=True)

                                tps = (args.quantity / (duration_ms / 1000)) if duration_ms > 0 else 0
                                operation = (
                                    "upload" if "upload-dir" in cmd else
                                    "download" if "download-all" in cmd else
                                    "delete" if "delete-all" in cmd else
                                    "list" if "list" in cmd else
                                    "unknown"
                                )

                                with open(args.output, "a") as f:
                                    bucket_id = bucket_id_map.get(bucket_name, bucket_name)
                                    operation_id = operation_id_map.get(operation, "0")
                                    f.write(f"{datetime.utcnow().timestamp()},{args.profile},{operation_id},{bucket_id},{size},{args.quantity},{args.workers},{duration_ms},{tps:.2f},{success}\n")


            print("Loop completo. Reiniciando...")

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário. Saindo.")

if __name__ == "__main__":
    main()

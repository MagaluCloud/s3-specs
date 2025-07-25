import os
import json
import csv
import time
import uuid
import argparse
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError


class ReplicatorTest:
    def __init__(
        self,
        bucket_name,
        profile_name="default",
        region=None,
        prefix="replicator-test/",
        wait_seconds=3600,
        failed_json_path="./output/failed_objects.json",
        output_csv_path="./output/replicator_results.csv"
    ):
        self.bucket_name = bucket_name
        self.profile_name = profile_name
        self.region = region
        self.prefix = prefix
        self.wait_seconds = wait_seconds
        self.failed_json_path = failed_json_path
        self.output_csv_path = output_csv_path

        config = Config(
            retries={
                "max_attempts": 1,
                "mode": "standard"
            },
            connect_timeout=3,
            read_timeout=3
        )

        session = boto3.Session(profile_name=self.profile_name)
        self.s3_client = session.client("s3", region_name=self.region, config=config)

    def upload_objects_until_failure(self):
        failed = []
        upload_success_count = 0  # contador de uploads bem-sucedidos

        while not failed:
            key = f"{self.prefix}obj_{uuid.uuid4().hex}.txt"
            content = f"Teste replicador - {datetime.utcnow().isoformat()}"
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=content.encode("utf-8")
                )
                print(f"[UPLOAD OK] {key}")
                upload_success_count += 1

                if upload_success_count % 1000 == 0:
                    print(f"[INFO] {upload_success_count} uploads bem-sucedidos. Executando cleanup...")
                    self.cleanup_bucket()

            except ClientError as e:
                error_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if error_code == 500:
                    print(f"[UPLOAD FAIL 500] {key}")
                    failed.append(key)
                    break
                else:
                    print(f"[IGNORED ERROR] {key} -> HTTP {error_code}")
                    continue

            except EndpointConnectionError as e:
                print(f"[CONN ERROR] {key} -> {str(e)} (ignorado)")
                continue

            except ReadTimeoutError as e:
                print(f"[TIMEOUT ERROR] {key} -> {str(e)} (ignorado)")
                continue


        if failed:
            os.makedirs(os.path.dirname(self.failed_json_path), exist_ok=True)
            with open(self.failed_json_path, "w") as f:
                json.dump(failed, f, indent=2)
            print(f"[INFO] Falhas salvas em {self.failed_json_path}")

        return failed

    def check_objects_exist(self, keys):
        paginator = self.s3_client.get_paginator("list_objects_v2")
        found_keys = set()
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                found_keys.add(obj["Key"])

        return [key for key in keys if key in found_keys]

    def write_csv_result(self, found, total):
        now = datetime.utcnow().timestamp()
        exists_count = len(found)

        header = ["timestamp", "total_missing", "found_after_wait"]
        write_header = not os.path.exists(self.output_csv_path)

        with open(self.output_csv_path, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if write_header:
                writer.writerow(header)
            writer.writerow([now, total, exists_count])

        print(f"[CSV] Resultado: {exists_count}/{total} encontrados após espera.")

    def cleanup_bucket(self):
        print("[CLEANUP] Limpando prefixo do bucket...")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        to_delete = []

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                to_delete.append({'Key': obj['Key']})

        if to_delete:
            self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': to_delete}
            )
            print(f"[CLEANUP] {len(to_delete)} objetos deletados.")
        else:
            print("[CLEANUP] Nenhum objeto para deletar.")

    def run(self):
        while True:
            print(f"[START] Subindo até encontrar erro de upload...")
            failed_keys = self.upload_objects_until_failure()

            if not failed_keys:
                print("[INFO] Nenhum erro encontrado. Tentando nova rodada...")
                continue

            print(f"[WAIT] Esperando {self.wait_seconds} segundos antes da verificação...")
            time.sleep(self.wait_seconds)

            print("[VALIDATION] Verificando se objetos com falha apareceram...")
            found = self.check_objects_exist(failed_keys)
            self.write_csv_result(found, total=len(failed_keys))

            self.cleanup_bucket()

            print(f"[CYCLE COMPLETE] Aguardando {self.wait_seconds} segundos antes de reiniciar...")
            time.sleep(self.wait_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teste replicador S3")
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3")
    parser.add_argument("--profile", default="default", help="Perfil AWS CLI")
    parser.add_argument("--region", default=None, help="Região AWS (opcional)")
    parser.add_argument("--prefix", default="replicator-test/", help="Prefixo S3 dos objetos")
    parser.add_argument("--total", type=int, default=10, help="Quantidade de uploads por rodada")
    parser.add_argument("--wait", type=int, default=3600, help="Tempo (segundos) para esperar antes da verificação")
    parser.add_argument("--fail-json", default="./output/failed_objects.json", help="Arquivo com objetos com falha")
    parser.add_argument("--output", default="./output/replicator_results.csv", help="Arquivo CSV de saída com os resultados")

    args = parser.parse_args()

    test = ReplicatorTest(
        bucket_name=args.bucket,
        profile_name=args.profile,
        region=args.region,
        prefix=args.prefix,
        wait_seconds=args.wait,
        failed_json_path=args.fail_json,
        output_csv_path=args.output
    )

    test.run()

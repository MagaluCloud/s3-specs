#!/bin/bash

# Exemplo de uso:
# ./prepare_list_buckets.sh bucket1 bucket2 10000 1

set -e

# Argumentos
BUCKET1=$1
BUCKET2=$2
QUANTITY=${3:-10000}
SIZE_KB=${4:-1}
WORKERS=${5:-256}

if [[ -z "$BUCKET1" || -z "$BUCKET2" ]]; then
    echo "Uso: $0 <bucket1> <bucket2> [quantidade] [tamanho_kb] [workers]"
    exit 1
fi

echo "Preparando buckets '$BUCKET1' e '$BUCKET2' com $QUANTITY arquivos de ${SIZE_KB}KB cada..."

# Cria arquivos temporários
TEMP_DIR=$(mktemp -d)
echo "Gerando arquivos em: $TEMP_DIR"

for i in $(seq 1 $QUANTITY); do
    printf '\0%.0s' $(seq 1 $((SIZE_KB * 1024))) > "$TEMP_DIR/file_${SIZE_KB}kb_${i}.txt"
done

# Função para criar bucket
create_bucket() {
    local bucket=$1
    echo "Criando $bucket"
    mgc object-storage buckets create $bucket
}

# Função para upload
upload_bucket() {
    local bucket=$1
    echo "Fazendo upload para: $bucket"
    mgc object-storage objects upload-dir "$TEMP_DIR" "${bucket}" --workers $WORKERS
}

# Criar os 2 buckets
create_bucket "$BUCKET1"
create_bucket "$BUCKET2"

# Upload para os dois buckets
upload_bucket "$BUCKET1"
upload_bucket "$BUCKET2"

# Limpeza
rm -rf "$TEMP_DIR"
echo "Buckets preparados com sucesso!"

# S3 Benchmark Script (`mgc-cli`)

A escolha de usar a `mgc-cli` foi por ela lidar melhor com paralelismo.

Este script realiza testes de benchmark de performance em buckets S3 utilizando o `mgc-cli`. Ele executa operações de **upload**, **download**, **delete** e **list**, salvando os resultados em um CSV com métricas como `tps` (transactions per second) e tempo de execução (`duration_ms`).

## Funcionalidades

- Executa testes repetidos de:
  - Upload de diretório com múltiplos arquivos
  - Download de todos os objetos de um prefixo
  - Delete de todos os objetos de um prefixo
  - Listagem de objetos de buckets

---

## Como executar

```bash
python continuous_benchmark.py \
  --buckets bucket1,bucket2 \
  --list-buckets bucket1-list,bucket2-list \
  --profile default \
  --sizes 1,10 \
  --quantity 1000 \
  --workers 256 \
  --times 3 \
  --output output/new_benchmark_results.csv
```

### Parâmetros

| Parâmetro           | Descrição                                                                 |
|---------------------|---------------------------------------------------------------------------|
| `--buckets`         | Lista separada por vírgula para todos os testes exceto **list**                   |
| `--list-buckets`    | Lista separada por vírgulas de buckets para o teste de **list**           |
| `--profile`         | Nome do perfil de credenciais AWS (default: `default`)                    |
| `--sizes`           | Tamanhos dos arquivos (em KB), separados por vírgulas (ex: `1,10,100`)     |
| `--quantity`        | Quantidade de arquivos a serem manipulados em cada operação               |
| `--workers`         | Número de workers paralelos para o `mgc-cli`                               |
| `--times`           | Número de repetições de cada operação                                     |
| `--output`          | Caminho do CSV de saída com os resultados                                 |

---

## Saída (CSV)

O script gera ou anexa ao arquivo `output/new_benchmark_results.csv` com o seguinte formato:

```csv
timestamp,region,operation,bucket,size,quantity,workers,duration_ms,tps,success
2025-07-12T14:00:00.000000+00:00,default,upload,my-bucket,1,1000,256,1580,632.91,1
```

- `timestamp`: Momento da execução
- `region`: Nome do perfil (substitui região aqui)
- `operation`: Tipo da operação (`upload`, `download`, `delete`, `list`)
- `bucket`: Nome do bucket utilizado
- `size`: Tamanho dos arquivos usados
- `quantity`: Número de arquivos
- `workers`: Workers utilizados na execução
- `duration_ms`: Tempo total em milissegundos
- `tps`: Transações por segundo (para todas exceto `list`)
- `success`: `1` para sucesso, `0` para erro

---

## Requisitos

- Python 3
- [mgc-cli](https://docs.magalu.cloud/docs/storage/object-storage/compatible-tools/mgc-cli-compatibility) instalado e funcional
- Permissões adequadas nos buckets
- Dependências:
  - Nenhuma extra (somente bibliotecas padrão do Python)

---

## Exemplo para preparar buckets de listagem

Utilize o script auxiliar para **popular os buckets** de listagem com objetos antes de rodar os testes `list`.

```bash
./prepare_list_buckets.sh bucket1 5000 1
./prepare_list_buckets.sh bucket2 5000 1
```

---

## Limpeza

O script limpa os diretórios temporários de download após cada operação.

---

## Observações

- O script roda **em loop infinito** até ser interrompido com `CTRL+C`.
- Ideal para testes contínuos de performance e regressão.

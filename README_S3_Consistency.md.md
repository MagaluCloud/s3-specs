# S3 Consistency Test Suite

Este projeto contém um conjunto de testes automatizados para avaliar a **consistência de leitura, replicação e remoção** de objetos em buckets compatíveis com S3.

Ele pode ser usado para validar diferentes comportamentos de consistência em buckets versionados, não versionados, com replicação entre regiões, etc.

---

## Índice

- [Testes CRUD](#testes-crud)
- [Replicator Test](#replicator-test)
- [Rotativo Test](#rotativo-test)
- [Overwriter Read Test](#overwriter-read-test)
- [Exportação para Prometheus](#exportação-para-prometheus)


## Dependencias
 - python
 - uv
---

# Testes CRUD

Testes de escrita, leitura e remoção com foco em consistência observável imediatamente após as operações.

## Testes Implementados

#### 1. `test_object_validations`
- **Objetivo:** Verificar se o objeto está disponível de forma consistente após o upload.
- **Validação:** O teste só considera o objeto consistente após **3 leituras consecutivas com sucesso** usando múltiplos comandos (`head-object`, `list-objects`, etc).
- **Quantidade padrão:** 512 objetos.
  
#### 2. `test_deleted_object_validations`
- **Objetivo:** Garantir que o objeto não seja mais encontrado após a exclusão.
- **Validação:** O objeto deve falhar consistentemente em **3 tentativas de leitura** após o `delete-object`.

---

### Execução (CRUD)

```bash
uv run pytest src/s3_specs/docs/consistency_test.py --config ./params.example.yaml --buckets bucket1,bucket2
```

## Saída e Métricas
Arquivo: `output/report_inconsistencies.csv`

```
quantity,workers,command,region,bucket_state,elapsed,attempts
512,256,head-object_after_put,se1,default,2.35,4
10,1,overwrite_10_times,se1,versioned,1.50,3
```

Este arquivo é exportado automaticamente e lido pelo exporter Prometheus.

---

# Replicator Test

Este teste contínuo simula uma situação onde um erro de upload ocorre (ex: erro 500), mas o objeto pode acabar sendo replicado mesmo assim.

## Objetivo
- Identificar falhas de replicação.
- Verificar se objetos "invisíveis" aparecem após o tempo de propagação.
- Gerar métricas para análise histórica e Prometheus.

## Lógica
- Envia objetos em loop até que uma falha ocorra.
- Salva os objetos que falharam.
- Espera (--wait segundos).
- Verifica se os objetos que falharam apareceram.
- Exporta o resultado.
- Repete o ciclo.

## Execução
```
python3 bin/replicator_consistency.py \
  --bucket my-replication-bucket \
  --profile se1
```

## Parâmetros Importantes

| Parâmetro     | Valor padrão                        | Descrição                                |
| ------------- | ----------------------------------- | ---------------------------------------- |
| `--bucket`    | **Obrigatório**                     | Bucket alvo                              |
| `--wait`      | `3600` (1 hora)                     | Tempo de espera entre erro e verificação |
| `--prefix`    | `"replicator-test/"`                | Prefixo para uploads                     |
| `--fail-json` | `"./output/failed_objects.json"`    | JSON de objetos que falharam             |
| `--output`    | `"./output/replicator_results.csv"` | CSV para Prometheus                      |


## Exemplo de saída

- JSON (falhas)
```
[
  "replicator-test/obj_d34db33f.txt"
]
```

- CSV
```
timestamp,bucket,prefix,total_missing,found_after_wait
2025-06-26T13:00:00Z,my-bucket,replicator-test/,1,1
```

---

# Rotativo Test

Esse script realiza uma verificação contínua com janela deslizante de consistência:
- Um novo objeto é adicionado.
- O mais antigo é deletado.
- A lista de objetos é comparada com o esperado.

## Objetivo
- Validar a consistência de leitura em buckets S3 (eventual ou forte).
- Detectar objetos faltantes ou presentes inesperadamente.

## Execução
```
python3 bin/continuous_consistency_monitor.py \
  --buckets bucket1,bucket2 \
  --object-limit 500 \
  --profile se1
```

## Parâmetros principais

| Parâmetro              | Padrão                            | Descrição                                      |
| ---------------------- | --------------------------------- | ---------------------------------------------- |
| `--buckets`            | **Obrigatório**                   | Buckets a serem monitorados                    |
| `--object-limit`       | `1000`                            | Número fixo de objetos ativos por bucket       |
| `--prefix`             | `"rotativo-test/"`                | Prefixo para uploads                           |
| `--profile`            | `"br-se1"`                        | Perfil AWS CLI                                 |
| `--max-workers`        | `100`                             | Uploads paralelos                              |
| `--threads-por-bucket` | `1`                               | Threads por bucket                             |
| `--csv-path`           | `"./output/rotativo_metrics.csv"` | Arquivo CSV de saída                           |
| `--debug`              | `False`                           | Escreve CSV mesmo se não houver inconsistência |


## Exemplo de saída (CSV)
```csv
timestamp,bucket,expected,found,missing,unexpected
2025-06-26T14:22:11Z,my-bucket,500,499,1,0
```

# Overwriter Read Test

## Objetivo
Avaliar a consistência de leitura após sobrescritas repetidas de um mesmo objeto S3.

## Funcionamento

- O mesmo objeto (com uma única chave) é sobrescrito diversas vezes com conteúdo diferente.
- Após cada sobrescrita, são feitas múltiplas leituras.
- O conteúdo retornado deve ser o mesmo da última escrita.

## Critério de sucesso
Cada sobrescrita é considerada consistente se todas as leituras subsequentes (ex: 3) retornarem exatamente o conteúdo esperado.

## Execução
```
uv run pytest src/s3_specs/docs/overwrite_consistency_test.py --config ./params.example.yaml
```

## Métricas exportadas:
O script grava os resultados no mesmo arquivo report_inconsistencies.csv com o seguinte formato:

```
quantity,workers,command,region,bucket_state,elapsed,attempts
10,1,overwrite_10_times,se1,versioned,1.24,3
```

# Exportação para Prometheus
O arquivo bin/metrics_exporter.py expõe todas as métricas em uma porta HTTP (default: :8000) para coleta pelo Prometheus.

Métricas disponíveis:

- s3_specs_status_counter
- s3_specs_time_metrics
- objs_consistency_time
- objs_benchmark
- s3_rotativo_inconsistencies
- replicator_consistency

Execute o exporter:
```
python3 bin/metrics_exporter.py
```

## Requisitos
- AWS CLI configurado (via --profile)
- Python 3.8+
- boto3, pytest, pandas, prometheus_client


## Recomendações
- Execute testes rotativos e replicadores continuamente (via systemd ou tmux).
- Ative o exporter Prometheus para observabilidade em tempo real.

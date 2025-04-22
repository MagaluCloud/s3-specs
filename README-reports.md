# Execução de Testes Automatizados

Este script permite executar testes automatizados por categoria e gerar relatórios.

## Como Executar os Testes

Para rodar os testes e gerar os relatórios, execute:

```bash
uv run python run_tests_and_generate_report.py <categoria>
```

## Parâmetros Disponíveis

- `categoria` (obrigatório): Define o escopo dos testes a serem executados. Pode ser um dos seguintes:

  ```
  full, acl, locking, policy, cold storage, basic, presign,
  versioning, multiple objects, big objects, consistency, benchmark
  ```

- `--mark` (opcional): Permite adicionar marcações extras do `pytest`, como `slow`, `serial`, etc.

## Exemplos de Uso

Executar todos os testes da categoria `basic`:

```bash
uv run python run_tests_and_generate_report.py basic
```


Executar todos os testes de todas as categorias:

```bash
uv run python run_tests_and_generate_report.py full
```

## Relatórios Gerados

HTML: Salvo na pasta reports_html/, com nome <categoria>.html.
PDF: Gerado na pasta reports_pdf/, contendo resumo dos testes.

## Acesso ao Dashboard Público

Os relatórios gerados na branch `main` estão disponíveis publicamente em:

 [https://magalucloud.github.io/s3-specs/](https://magalucloud.github.io/s3-specs/)

Esse dashboard é atualizado automaticamente com os relatórios HTML mais recentes após a execução dos workflows.

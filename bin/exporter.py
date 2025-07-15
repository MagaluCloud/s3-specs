from prometheus_client import start_http_server, Gauge, Counter
import pandas as pd
import argparse
import time
import glob
import os

parser = argparse.ArgumentParser()
parser.add_argument('--parquet_path',
                    default='./output/tests.parquet',
                    required=False,
                    help='Path of folder containing the execution_time and test parquet artifacts')


args = parser.parse_args()

exported_keys = set()

paths = {
    'report_folder': './output/',
    'grouped_file': './output/resultado_grouped.csv',
    'inconsistencies_file': './output/report_inconsistencies.csv',
    'benchmark_file': './output/benchmark_results.csv',
    'rotativo_metrics_file': './output/rotativo_metrics.csv',
    'replicator_file': './output/replicator_results.csv'
}

# Definir as métricas Gauge
replicator_gauge = Gauge(
    'replicator_consistency',
    'Métricas de consistência em replicação',
    ['timestamp', 'bucket', 'prefix', 'total_missing', 'found_after_wait']
)

objs_consistency_time = Gauge(
    'objs_consistency_time',
    'Tempo de execução para diferentes operações de consistência',
    ['quantity', 'workers', 'command', 'region', 'bucket_state', 'elapsed', 'attempts']
)

avg_gauge = Gauge(
    'objs_benchmark',
    'Tempo médio de operações',
    ['region', 'tool', 'size', 'times', 'workers', 'quantity', 'operation']
)

execution_time_gauge = Gauge(
    's3_specs_time_metrics',
    'Tests time metrics',
    ['name', 'execution_type', 'category','time_metric']
)

execution_status_counter = Counter(
    's3_specs_status_counter',
    'Counter containing the number of status ocurrences on the recurrent testing',
    ['name', 'status', 'category']
)

rotativo_gauge = Gauge(
    's3_rotativo_inconsistencies',
    'Inconsistências por execução nos testes rotativos de consistência (bucket é um ID numérico)',
    ['bucket', 'timestamp', 'type']
)

def read_csv_and_update_metrics():
    # Limpe as métricas existentes
    objs_consistency_time.clear()
    avg_gauge.clear()

    # Processar o report-inconsistencies.csv (Novo formato)
    inconsistencies_file = paths.get('inconsistencies_file')
    if os.path.exists(inconsistencies_file):
        print(f'Processing file: {inconsistencies_file}')
        df = pd.read_csv(inconsistencies_file)

        if 'command' in df.columns:
            for _, row in df.iterrows():
                if pd.notna(row['command']) and pd.notna(row['elapsed']):
                    labels = {
                        'quantity': str(row['quantity']),
                        'workers': str(row['workers']),
                        'command': row['command'],
                        'region': row['region'],
                        'bucket_state': row['bucket_state'],
                        'elapsed': str(row['elapsed']),
                        'attempts': str(row['attempts'])
                    }
                    objs_consistency_time.labels(**labels).set(row['elapsed'])
    else:
        print(f"Arquivo {inconsistencies_file} não encontrado.")

    # Processar o processed_data.csv (Formato anterior)
    processed_files = glob.glob(paths.get('benchmark_file'))
    if processed_files:
        latest_processed_file = max(processed_files, key=os.path.getmtime)
        print(f'Processing file: {latest_processed_file}')
        df = pd.read_csv(latest_processed_file)

        if 'operation' in df.columns:
            df_grouped = df.groupby(['region', 'tool', 'size', 'times', 'workers', 'quantity', 'operation'])['time'].median().reset_index()
            df_grouped.to_csv(paths.get('grouped_file'), index=False)
            print("Arquivo 'resultado_grouped.csv' com as medianas gerado.")

            for _, row in df_grouped.iterrows():
                labels = {
                    'region': row['region'],
                    'tool': row['tool'],
                    'size': str(row['size']),
                    'times': str(row['times']),
                    'workers': str(row['workers']),
                    'quantity': str(row['quantity']),
                    'operation': row['operation'],
                }
                avg_gauge.labels(**labels).set(row['time'])
    else:
        print("Nenhum arquivo benchmark_results.csv encontrado.")

def execution_time_metrics_exporter():
    file_path = os.path.join(args.parquet_path, 'execution_time.parquet')
    tests_file_path = os.path.join(args.parquet_path, 'tests.parquet')

    try:
        df_category = pd.read_parquet(file_path)
    except FileNotFoundError:
        print(f"Arquivo {file_path} não encontrado.")
        return

    try:
        df_tests = pd.read_parquet(tests_file_path)
    except FileNotFoundError:
        print(f"Arquivo {tests_file_path} não encontrado.")
        return

    # Merge to retrieve the categories
    df_category = df_category.merge(
        df_tests,
        how='inner',
        left_on=['execution_name', 'execution_datetime'],
        right_on=['name', 'execution_datetime']
    ).drop_duplicates()

    # Get useful columns
    cleaned_time_metric_df = df_category[['name', 'category', 'execution_type', 'avg_time', 'min_time', 'total_time']]

    # Melt o DataFrame
    melted_df = pd.melt(
        cleaned_time_metric_df,
        id_vars=['name', 'execution_type', 'category'],
        value_vars=['avg_time', 'min_time', 'total_time'],
        var_name='time_metric',
        value_name='time_values'
    ).reset_index(drop=True)


    # Set metrics
    for record in melted_df.to_dict('records'):
        execution_time_gauge.labels(
            name=record['name'],
            execution_type=record['execution_type'],
            category=record['category'],
            time_metric=record['time_metric'],
        ).set(record['time_values'])

    print('Time metrics exported...')

def test_metrics_exporter():
    file_path = os.path.join(args.parquet_path, 'tests.parquet')

    try:
        df = pd.read_parquet(file_path)
    except FileNotFoundError:
        print(f"Arquivo {file_path} não encontrado.")
        return

    # Define the status mapping (optional, depending on how you want to track)
    status_mapping = {
        'PASSED': 'passed',
        'SKIPPED': 'skipped',
        'FAILED': 'failed',
        'ERROR': 'error',
    }

    # Clean the dataframe
    cleaned_status_df = df.drop(columns=['artifact_name', 'execution_datetime', 'arguments'])

    # Convert status to more readable labels if needed
    cleaned_status_df['status'] = cleaned_status_df['status'].map(status_mapping)

    # Increment the counter for each status occurrence
    for _, row in cleaned_status_df.iterrows():
        execution_status_counter.labels(
            name=row['name'],
            category=row['category'],
            status=row['status']
        ).inc(1)

        print("Test metrics exported...")
    else:
        print(f"Arquivo {file_path} não encontrado. Nenhum teste foi exportado.")

def delete_temp_parquets():
    # deleting temporary parquets
    parquets_paths = 'output'

    try:
        print(f"Deleting all parquets...")
        for filename in os.listdir(parquets_paths):
            if filename.endswith('.parquet'):
                file_path = os.path.join(parquets_paths, filename)
                os.remove(file_path)
    except Exception as e:
        print(f"Error occurred while deleting parquets: {e}")

def export_rotativo_metrics():
    csv_path = paths.get('rotativo_metrics_file')

    if not os.path.exists(csv_path):
        print(f"Arquivo {csv_path} não encontrado.")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Erro ao ler {csv_path}: {e}")
        return

    if df.empty:
        print(f"CSV {csv_path}. Nenhuma métrica exportada.")
        return

    # Sanitize campos
    df['bucket'] = df['bucket'].astype(str).str.strip()
    df['timestamp'] = df['timestamp'].astype(str).str.strip().str.split('.').str[0]

    rotativo_gauge.clear()
    novas_metricas = 0

    for _, row in df.iterrows():
        key = f"{row['bucket']}:{row['timestamp']}"
        if key in exported_keys:
            continue

        exported_keys.add(key)

        bucket = row['bucket']
        timestamp = row['timestamp']
        rotativo_gauge.labels(bucket=bucket, timestamp=timestamp, type='missing').set(float(row['missing']))
        rotativo_gauge.labels(bucket=bucket, timestamp=timestamp, type='unexpected').set(float(row['unexpected']))

        novas_metricas += 1

    print(f"Rotativo metrics exported ({novas_metricas} novas).")

def export_replicator_metrics():
    csv_path = paths.get('replicator_file')

    if not os.path.exists(csv_path):
        print(f"Arquivo {csv_path} não encontrado.")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Erro ao ler {csv_path}: {e}")
        return

    if df.empty:
        print(f"CSV {csv_path}. Nenhuma métrica exportada.")
        return

    replicator_gauge.clear()

    for _, row in df.iterrows():
        replicator_gauge.labels(
            timestamp=row['timestamp'],
            total_missing=int(row['total_missing']),
            found_after_wait=int(row['found_after_wait'])
        ).set(1)

    print("Replicator metrics exported.")


if __name__ == '__main__':
    start_http_server(8000)
    while True:
        # Retrieving metrics
        read_csv_and_update_metrics()
        test_metrics_exporter()
        execution_time_metrics_exporter()
        export_rotativo_metrics()
        delete_temp_parquets()
        export_replicator_metrics()

        time.sleep(3600)  # Atualize a cada 3600 segundos (1 hora)

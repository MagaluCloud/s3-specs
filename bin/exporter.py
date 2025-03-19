from prometheus_client import start_http_server, Gauge
import pandas as pd
import argparse
import time
import glob
import os

# Definir as métricas Gauge
objs_consistency_time = Gauge(
    'objs_consistency_time',
    'Tempo de execução para diferentes operações de consistência',
    ['quantity', 'workers', 'command', 'region', 'bucket_state', 'elapsed']
)

avg_gauge = Gauge(
    'objs_benchmark',
    'Tempo médio de operações',
    ['region', 'tool', 'size', 'times', 'workers', 'quantity', 'operation','time']
)

execution_time_gauge = Gauge(
    's3_specs_time_metrics', 
    'Tests time metrics',  
    ['execution_name', 'execution_type', 'time_metric']  
)

execution_status_gauge = Gauge(
    's3_specs_status_data',  
    'Test statuses', 
    ['name', 'category']  
)

parser = argparse.ArgumentParser()
parser.add_argument('--parquet_path',
                    required=True, 
                    help='Path of folder containing the execution_time and test parquet artifacts')

def read_csv_and_update_metrics():
    report_folder = os.path.abspath("./report")

    # Limpe as métricas existentes
    objs_consistency_time.clear()
    avg_gauge.clear()

    # Processar o report-inconsistencies.csv (Novo formato)
    inconsistencies_file = os.path.join(report_folder, 'report-inconsistencies.csv')
    if os.path.exists(inconsistencies_file):
        print(f'Processing file: {inconsistencies_file}')
        df = pd.read_csv(inconsistencies_file)

        # Verificar o formato do CSV e processar adequadamente
        if 'command' in df.columns:
            for _, row in df.iterrows():
                # Verificar se a linha contém valores válidos
                if pd.notna(row['command']) and pd.notna(row['elapsed']):
                    labels = {
                        'quantity': str(row['quantity']),
                        'workers': str(row['workers']),
                        'command': row['command'],
                        'region': row['region'],
                        'bucket_state': row['bucket_state'],
                        'elapsed': str(row['elapsed'])
                    }
                    objs_consistency_time.labels(**labels).set(row['elapsed'])

    else:
        print(f"Arquivo {inconsistencies_file} não encontrado.")

    # Processar o processed_data.csv (Formato anterior)
    processed_files = glob.glob(os.path.join(report_folder, 'benchmark_results.csv'))
    if processed_files:
        latest_processed_file = max(processed_files, key=os.path.getmtime)
        print(f'Processing file: {latest_processed_file}')
        df = pd.read_csv(latest_processed_file)

        # Verificar o formato do CSV e processar adequadamente
        if 'operation' in df.columns:
            # Agrupar pelos primeiros 7 campos e calcular a mediana do 'time'
            df_grouped = df.groupby(['region', 'tool', 'size', 'times', 'workers', 'quantity', 'operation'])['time'].median().reset_index()

            # Exportar o arquivo agrupado com a mediana
            df_grouped.to_csv('report/resultado_grouped.csv', index=False)
            print("Arquivo 'resultado_grouped.csv' com as medianas gerado.")

            # Processar os dados para o 'avg_gauge'
            for _, row in df_grouped.iterrows():
                labels = {
                    'region': row['region'],
                    'tool': row['tool'],
                    'size': str(row['size']),
                    'times': str(row['times']),
                    'workers': str(row['workers']),
                    'quantity': str(row['quantity']),
                    'operation': row['operation'],
                    'time': str(row['time'])
                }
                avg_gauge.labels(**labels).set(row['time'])

    else:
        print("Nenhum arquivo benchmark_results.csv encontrado.")

def execution_metrics_exporter():
    file_path = 'execution_time.parquet'
    file_path = os.path.join(parser.parquet_path, file_path)
    df = pd.read_parquet(file_path)

    # Drop unnecessary columns and take the first 5 rows
    cleaned_time_metric_df = df.drop(columns=['execution_datetime', 'number_runs'])

    # Melt the DataFrame
    melted_df = pd.melt(
        cleaned_time_metric_df,
        id_vars=['execution_name', 'execution_type'],  
        value_vars=['avg_time', 'min_time', 'total_time'],  
        var_name='time_metric', 
        value_name='time_values'  
    ).reset_index(drop=True).drop_duplicates()

    # Set gauge values
    for record in melted_df.to_dict('records'):
        execution_time_gauge.labels(
            execution_name=record['execution_name'],
            execution_type=record['execution_type'],
            time_metric=record['time_metric']
        ).set(record['time_values'])

    print('Time metrics exported...')

def test_metrics_exporter():
    # Test data gauge
    file_path = 'tests.parquet'
    file_path = os.path.join(parser.parquet_path, file_path)
    df = pd.read_parquet(file_path)

    status_to_numeric = {
        'PASSED': 1.0,
        'FAILED': -1.0,
        'ERROR': -2.0,
    }

    cleaned_status_df = df.drop(columns=['artifact_name', 'execution_datetime', 'arguments'])
    cleaned_status_df['status'] = cleaned_status_df['status'].map(status_to_numeric).drop_duplicates()
    dicts_time_metric_df = cleaned_status_df.to_dict(orient='records')

    # Set gauge values
    for record in dicts_time_metric_df:
        execution_status_gauge.labels(
            name=record['name'],  # Pass 'Name' label
            category=record['category']  # Pass 'Category' label
        ).set(record['status'])  # Set the numeric status value
    print('Test metrics exported...')


if __name__ == '__main__':
    start_http_server(8000)
    while True:
        read_csv_and_update_metrics()
        test_metrics_exporter()
        execution_metrics_exporter()
        time.sleep(600)  # Atualize a cada 600 segundos (10 minutos)

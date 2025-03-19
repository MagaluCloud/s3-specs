from prometheus_client import start_http_server, Gauge
import pandas as pd
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

if __name__ == '__main__':
    start_http_server(8000)
    while True:
        read_csv_and_update_metrics()
        time.sleep(600)  # Atualize a cada 600 segundos (10 minutos)

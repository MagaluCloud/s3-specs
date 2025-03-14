from prometheus_client import start_http_server, Gauge
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor


output_path = '../output/'

def execution_metrics_exporter():
    file_path = output_path + 'execution_time.parquet'
    df = pd.read_parquet(file_path)

    # Drop unnecessary columns and take the first 5 rows
    cleaned_time_metric_df = df.drop(columns=['execution_datetime', 'number_runs']).head(5)

    # Melt the DataFrame
    melted_df = pd.melt(
        cleaned_time_metric_df,
        id_vars=['execution_name', 'execution_type'],  
        value_vars=['avg_time', 'min_time', 'total_time'],  
        var_name='type_time', 
        value_name='time_values'  
    ).reset_index(drop=True).drop_duplicates()

    # Gauge setup
    execution_time_gauge = Gauge(
        's3_specs_time_metrics', 
        'Tests time metrics',  
        ['execution_name', 'execution_type', 'type_time']  
    )

    # Set gauge values
    for record in melted_df.to_dict('records'):
        execution_time_gauge.labels(
            Execution_name=record['execution_name'],
            Execution_type=record['execution_type'],
            type_time=record['type_time']
        ).set(record['time_values'])

    print('Time metrics exported...')

def test_metrics_exporter():
    # Test data gauge
    file_path = output_path + 'tests.parquet'
    df = pd.read_parquet(file_path)

    status_to_numeric = {
        'PASSED': 1.0,
        'FAILED': -1.0,
        'ERROR': -2.0,
    }

    cleaned_status_df = df.drop(columns=['artifact_name', 'execution_datetime', 'arguments']).head(5)
    cleaned_status_df['status'] = cleaned_status_df['status'].map(status_to_numeric).drop_duplicates()
    dicts_time_metric_df = cleaned_status_df.to_dict(orient='records')

    # Gauge setup
    execution_status_gauge = Gauge(
        's3_specs_status_data',  
        'Test statuses', 
        ['name', 'category']  
    )

    # Set gauge values
    for record in dicts_time_metric_df:
        execution_status_gauge.labels(
            name=record['name'],  # Pass 'Name' label
            category=record['category']  # Pass 'Category' label
        ).set(record['status'])  # Set the numeric status value
    print('Test metrics exported...')


if __name__ == '__main__':

    # Start the HTTP server on port 8000
    print('Starting server...')
    start_http_server(8000)
    while True:
        test_metrics_exporter()
        execution_metrics_exporter()
        print('Exporting Parquets...')
        time.sleep(600)  # Update every 10 seconds
from prometheus_client import start_http_server, Gauge
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor


def read_parquet_file(file_path):
    """Read a single Parquet file and return its DataFrame."""
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
   
        return None

def execution_metrics_exporter():
    file_path = '/home/henrique.luis/repos/s3-specs/reports/output/execution_time.parquet'
    df = read_parquet_file(file_path)

    # Drop unnecessary columns and take the first 5 rows
    cleaned_time_metric_df = df.drop(columns=['Execution_Datetime', 'Number_Runs']).head(5)

    # Melt the DataFrame
    melted_df = pd.melt(
        cleaned_time_metric_df,
        id_vars=['Execution_name', 'Execution_type'],  # Columns to keep as-is
        value_vars=['Avg_Time', 'Min_Time', 'Total_Time'],  # Columns to merge
        var_name='type_time',  # New column name for the time types
        value_name='time_values'  # New column name for the time values
    ).reset_index(drop=True).drop_duplicates()

    # Gauge setup
    execution_time_gauge = Gauge(
        's3_specs_time_metrics', 
        'Tests time metrics',  
        ['Execution_name', 'Execution_type', 'type_time']  
    )

    # Set gauge values
    for record in melted_df.to_dict('records'):
        execution_time_gauge.labels(
            Execution_name=record['Execution_name'],
            Execution_type=record['Execution_type'],
            type_time=record['type_time']
        ).set(record['time_values'])

    print('Time metrics exported...')

def test_metrics_exporter():
    # Test data gauge
    file_path = '/home/henrique.luis/repos/s3-specs/reports/output/tests.parquet'
    df = read_parquet_file(file_path)

    status_to_numeric = {
        'PASSED': 1.0,
        'FAILED': -1.0,
        'ERROR': -2.0,
    }


    cleaned_status_df = df.drop(columns=['Artifact_Name', 'Execution_Datetime', 'Arguments']).head(5)
    cleaned_status_df['Status'] = cleaned_status_df['Status'].map(status_to_numeric).drop_duplicates()
    dicts_time_metric_df = cleaned_status_df.to_dict(orient='records')

    # Gauge setup
    execution_status_gauge = Gauge(
        's3_specs_status_data',  
        'Test statuses', 
        ['Name', 'Category']  
    )

    # Set gauge values
    for record in dicts_time_metric_df:
        execution_status_gauge.labels(
            Name=record['Name'],  # Pass 'Name' label
            Category=record['Category']  # Pass 'Category' label
        ).set(record['Status'])  # Set the numeric status value
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
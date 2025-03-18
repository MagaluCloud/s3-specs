from prometheus_client import start_http_server, Gauge
import pandas as pd
import time
import argparse
import os


execution_time_gauge = Gauge(
    's3_specs_time_metrics', 
    'Tests time metrics',  
    ['execution_name', 'execution_type', 'time_metric']  
)

# Gauge setup
execution_status_gauge = Gauge(
    's3_specs_status_data',  
    'Test statuses', 
    ['name', 'category']  
)


parser = argparse.ArgumentParser()
parser.add_argument('--parquet_path',
                    required=True, 
                    help='Path of folder containing the execution_time and test parquet artifacts')

parser = parser.parse_args()  # Parse the arguments

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

    # Start the HTTP server on port 8000
    print('Starting server...')
    start_http_server(8000)
    while True:
        test_metrics_exporter()
        execution_metrics_exporter()
        print('Exporting Parquets...')
        time.sleep(600)  # Update every 10 seconds
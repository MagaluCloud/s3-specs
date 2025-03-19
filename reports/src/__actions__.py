import pandas as pd
from logDataclasses import TestData
from logExtractor import PytestArtifactLogExtractor
from ghActionsScrapper import ActionsWorkflow, ActionsJobs, ActionsArtifacts
import argparse
import inspect

def parser_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo_path',
                        required=True, 
                        default='*',
                        help='Github repository name')
    parser.add_argument('--query_size',
                    required=True, 
                    default='*',
                    help='Number of workflows to be downloaded')

    return parser.parse_args()  # Parse the arguments

if __name__ == '__main__':
    # Parse command-line arguments
    parser = parser_arguments()

    # Scrape the last n workflows and download their artifacts
    workflow = ActionsWorkflow(repository=parser.repo_path, query_size=parser.query_size)
    artifacts = ActionsArtifacts(databaseIds=workflow.df['databaseId'].values, repository=parser.repo_path)

    test_data_arguments = list(inspect.signature(TestData).parameters.keys())
    test_data = {args: [] for args in test_data_arguments}

    for path in artifacts.paths:
        logs = PytestArtifactLogExtractor(path).log_to_df()
        # Adding the new tuple to the dict
        if test_data:
            list(map(lambda key, log: test_data[key].append(log), test_data_arguments, logs))

    test_data = TestData(**test_data)






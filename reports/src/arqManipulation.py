import os
import pandas as pd
import re
import json

class ArqManipulation:
    """
    A utility class for file operations and data manipulation.
    """

    @staticmethod 
    def read_parquet_file(parquet_file_name: str) -> pd.DataFrame:
        """
        Reads a Parquet file and returns a DataFrame.

        :param parquet_file_name: Path to the Parquet file.
        :return: DataFrame with file contents.
        """
        try:
            if not os.path.exists(parquet_file_name):
                return pd.DataFrame()
            
            return pd.read_parquet(parquet_file_name)
        except Exception as e:
            raise RuntimeError(f"Error reading Parquet file '{parquet_file_name}': {e}")

    @staticmethod
    def save_df_to_parquet(df: pd.DataFrame, parquet_file_name: str):
        """
        Saves a DataFrame to a Parquet file.

        :param df: Dataframe to save.
        :param parquet_file_name: Parqueet saving path.
        """
        try:
            print(f"Saving {parquet_file_name}")
            os.makedirs(os.path.dirname(parquet_file_name), exist_ok=True)
            df.to_parquet(parquet_file_name)
        except Exception as e:
            raise RuntimeError(f"Error saving DataFrame to Parquet file '{parquet_file_name}': {e}")

    @staticmethod
    def clean_ansi_escape(base_str: str) -> str:
        """
        Removes ANSI escape values from a string.

        :param base_str: Unformmated string.
        :return: Cleaned string.
        """
        return re.sub(r'\x1B\[[0-9;]*[A-Za-z]', '', base_str)

    @staticmethod
    def parse_stdout_json(base_str: str) -> dict:
        """
        Parses JSON output from GitHub CLI after cleaning ANSI escape sequences.

        :param base_str: The raw output string from the GitHub CLI.
        :return: Parsed JSON dictionary.
        """
        try:
            cleaned = ArqManipulation.clean_ansi_escape(base_str)
            str_output = ''.join(cleaned.splitlines())
            return json.loads(str_output)
        except json.JSONDecodeError as e:
            raise e

    @staticmethod
    def json_to_df(parsed_json: dict) -> pd.DataFrame:
        """
        Converts a JSON dictionary to a sorted DataFrame with specific columns.

        :param parsed_json: Parsed JSON data.
        :return: Pandas DataFrame sorted by the 'createdAt' column.
        """
        try:
            df_json = pd.DataFrame(parsed_json)
            required_columns = ['name', 'createdAt', 'conclusion', 'status', 'databaseId', 'workflowDatabaseId']
            
            if not all(col in df_json.columns for col in required_columns):
                raise KeyError(f"Missing required columns in JSON data: {set(required_columns) - set(df_json.columns)}")

            df_json['createdAt'] = pd.to_datetime(df_json['createdAt'])
            return df_json[required_columns].sort_values(by="createdAt")
        except KeyError as e:
            raise ValueError(f"Error processing JSON to DataFrame: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error in json_to_df: {e}")

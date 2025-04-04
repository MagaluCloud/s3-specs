import pandas as pd
import numpy as np
import re
from logDataclasses import ExecutionEntity, Artifact, ArtifactInfo, Tests, ExecutionTime, Failures, TestData
from numpy.lib.stride_tricks import sliding_window_view as swv
from datetime import datetime
from arqManipulation import ArqManipulation 
from dataclasses import fields

paths = {
    'status':'./output/pytest.status.log.parquet',
    'categories':'./output/pytest.categories.log.parquet',
    'failures':'./output/pytest.failures.log.parquet',
    }

class PytestArtifactLogExtractor:
    """f
    A class to extract and process test status and timing information from a pytest artifact log.
    """
    def __init__(self, path: str):
        """
        Initializes the PytestArtifactLogExtractor object.

        :param path: Path to the pytest artifact log file.
        """
        self.path = path
        self.local = True
        self.data = self.__read_file__()

    def __read_file__(self):
        """
        Reads the contents of the log file and returns it as a string.

        :return: String containing the file content.
        """
        with open(self.path, "r") as file: 
            data = file.read()

        return ArqManipulation.clean_ansi_escape(data)

    def print_dataclass(self, data):
        print(f"\n{data.__class__.__name__} values:")
        for field in fields(data):
            print(f"{field.name}: {getattr(data, field.name)}")

    def log_to_df(self):
        """
        Parses the log file to extract test results and performance metrics.

        :return: A DataFrame combining test statuses with time metrics.
        """
        execution_entity, artifact = self.__extract_artifact_info__()
        tests, execution_time, failures = self.__extract_all_categories__(execution_entity, artifact)

        return execution_entity, artifact, tests, execution_time, failures
     
    def __get_list_by_name__(self, data: list, name: str):
        """
        Find the sublist containing the specified name in the first element.

        :param data: A list of sublists to search through.
        :type data: list[list]
        :param name: The name to search for in the first element of each sublist.
        :type name: str
        :return: A list of sublists where the first element matches the name.
        :rtype: list[list]
        """
        matching_sublists = []
        
        for sublist in data:
            if re.search(name, sublist[0]):  # Converte os itens para string
                matching_sublists.append(sublist)
        
        return matching_sublists

    def __extract_all_categories__(self, execution_entity, artifact):
        """
        Converts extracted timing data into DataFrames.

        :param values: A list of lists with extracted time metrics.
        :type values: list[list]
        :return: A list of DataFrames with execution time statistics.
        """
        header = []
        # Filtering out irrelevant categories
        keywords = ('deselected', 'passed in', 'grand total', 'live log')

        # Breaking each file into an list that contains the category as the pos 0
        values = self.data.splitlines()
        for value in values:
            if any(k in value for k in keywords):
                continue   
            elif re.match(r'=+|-+', value): # Divide by headers demarked by '=' or '-' (logging)
                value = value.replace("=", "")  
                value = value.replace("-", "")  
                header.append([value]) 
            else:
                # Populate each category and break in the case of the pytest-durations tables while ignoring empty values
                header[-1].append(value)

        headers = [['live_log','live_log','live_log', 'live_log']]
        # Ignore cases of logging mode is active
        if not 'live log' in self.data:
            headers = self.__extract_test_status_names__(self.__get_list_by_name__(header, 'test session')[0])    

        tests = [Tests( artifact_name=artifact.name, 
                        execution_datetime=execution_entity.execution_datetime,
                        status=t[0].strip(), 
                        category=t[1].split("/")[-1].strip().replace('.py', ''), # Formatting for readability
                        name=t[2].strip(), 
                        arguments=t[3] if t[3] else None)  for t in headers]

        # Execution_time
        execution_time_df = self.__create_time_df__(self.__extract_time_categories__(self.__get_list_by_name__(header, 'duration top')))

        execution_time = [ExecutionTime(
            execution_datetime=execution_entity.execution_datetime,  # Assuming this is a fixed value
            execution_name=row['name'],
            execution_type=row['durationType'],
            number_runs=row['num'],
            avg_time=row['avg'],
            min_time=row['min'],
            total_time=row['total']
        ) for _, row in execution_time_df.iterrows()]   

        failures_df = self.__create_failure_df__(self.__extract_failures_errors__(self.__get_list_by_name__(header, 'summary')))

        if not failures_df.empty:
            failures = [Failures(test_name= row['test_name'],
                                artifact_name=artifact.name,
                                execution_datetime=execution_entity.execution_datetime,
                                error=row['error'],
                                details=row['details'],
            ) for _, row in failures_df.iterrows()]
        else:
            failures = [Failures(test_name=None,artifact_name=None,execution_datetime=execution_entity.execution_datetime,error=None,details=None)]

        return tests, execution_time, failures

    def __extract_test_status_names__(self, data):
        """
        Extracts the status and the tests names out of the pytest log, breaking them down to a list of lists.

        :param data: A list of lines containing test results.
        :type data: list[str]
        :return: list[list[str]]: test_names, statuses(PASSED, FAILED, ERROR), category, arguments.
        """

        tests = []
        keywords = ('PASSED', 'FAILED', 'ERROR', 'SKIPPED')

        for line in data:
            if any(k in line for k in keywords):
                match = re.search(r'(PASSED|FAILED|ERROR|SKIPPED).*', line).group()
                # Splitting the Keyword NameTest from category and argument
                match = re.split(r'::', match, 1)
                tmp = re.split(r'\s', match[0], maxsplit=1)
                # Splitting the category from arguments
                tmp += re.split(r'\[', match[1], maxsplit=1)
                # Allow degenerated data to fit in the dataframe
                while(len(tmp) < 4):
                    tmp.append(None)

                tests.append(tmp)

        return tests
    
    def __extract_time_categories__(self, data):
        categories = []
        for d in data:
            categories.append([])
            for s in d: 
                formatted_s = list(filter(None,s.split(" ")))
                if 'duration' in formatted_s: #converting the header name back into string
                    formatted_s = ' '.join(formatted_s)
                categories[-1].append(formatted_s)

        return categories

    def __extract_failures_errors__(self, data):

        """
        Extracts from the pytest log the details of tests with failures or errors cleaning the data to make it ready to a dataframe.

        :param data: A list of strings containing test results.
        :type data: list[str]
        :return: list[list]: A list of lists containing details of tests with failures and/or errors.
        """
        # Some test wont have errors, but there still need a dataframe
        if not data:
            return [[None]*5]


        keywords = ['PASSED','FAILED','ERROR']
        failures = []

        for line in data[0]:
            if any(k in line for k in keywords):
                final_failure_list = []
                keywords_string = (re.search(r'(PASSED|FAILED|ERROR|SKIPPED).*', line).group())
                status_errors = re.split(r'::', keywords_string, 1)

                status_category = re.split(r'\s', status_errors[0], maxsplit=1)
                final_failure_list += status_category
                
                test_name = re.split(r'\[.*?\] - | - ', status_errors[1], maxsplit=2)
                final_failure_list.append(test_name[0])
                error = re.split(r'(?<=.): ', test_name[1], maxsplit=1)
                final_failure_list += error

                # Allow degenerated data to fit in the dataframe
                while(len(final_failure_list) < 5):
                    final_failure_list.append(None)

                failures.append(final_failure_list)

        return failures

    def __create_status_df__(self, data):
        formatted_data = []

        try:
            for d in data:
                if 'live_log' not in d:
                    formatted_data.append(d)

            df = pd.DataFrame(formatted_data, columns=["status", "category", "name", "arguments"])
            df['name'] = df['name'].astype(str).str.replace(" ", "", regex=True)
        except:
            print('None Type')
            return pd.DataFrame()
        return df

    def __create_time_df__(self, data):
        """
        Converts extracted timing information into DataFrames.

        :param values: A list of lists containing extracted time metrics.
        :return: A list of DataFrames with execution time statistics.
        """
        dfs = pd.DataFrame()
        
        for h in data:
            time_df = pd.DataFrame(h[2:], columns=h[1])

            # Converting time-related columns to datetime.time format
            time_columns = ['avg', 'min', 'total']
            for col in time_columns:
                if col in time_df.columns:
                    time_df[col] = pd.to_timedelta(time_df[col], errors='coerce').dt.total_seconds().round(3)
                    
            # Assigning a 'durationType' column for metric categorization
            time_df['durationType'] = h[0].replace('top', '').replace('test', '')

            dfs = pd.concat([time_df, dfs], ignore_index=True)

        return dfs

    def __create_failure_df__(self, data):
        return pd.DataFrame(data, columns=['status', 'category', 'test_name', 'error', 'details']).dropna()

    def __extract_artifact_info__(self):
        """
        Extracts test and database ID information from the log file path.

        :return: A DataFrame containing 'test' and 'databaseId' information.
        """
        
        # Extract filename without extension format = (testName.endpoint.datetime.fileExtension)
        stripped = self.path.split('/')[-1].split('.', maxsplit=3)
        if len(stripped) != 4:
            print(f"Filename: {self.path}\n")
            raise "Artifact name format must be: 'testName.endpoint.datetime.fileExtension' with datetime = 'YYYYMMDDTHHmmSS'"

        datetime_file = parse_datetime(stripped[2])

        # Ensure there are exactly three elements (fill missing ones with None)
        execution_entity = ExecutionEntity(execution_datetime=datetime_file,endpoint=stripped[1])
        artifact = Artifact(name=stripped[0], execution_datetime=execution_entity.execution_datetime)

        return execution_entity, artifact

def parse_datetime(datetime_str):
    """Parse a datetime string with multiple possible formats into a numpy datetime64 object.
    
    param: datetime_str: String representing a datetime in one of the following formats: %Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%dT%H%M%S%f"
    return: np.datetime64: Parsed datetime object or current datetime if parsing fails
    """
    format_templates = [
        "%Y-%m-%dT%H:%M:%S.%f",  
        "%Y-%m-%d %H:%M:%S.%f",  
        "%Y%m%dT%H%M%S%f",       
    ]
    
    for fmt in format_templates:
        try:
            dt = datetime.strptime(datetime_str, fmt)
            return np.datetime64(dt)
        except ValueError:
            continue
    
    print(f"Warning: {datetime_str} doesn't match any known format, using current datetime")
    return np.datetime64(datetime.now())
from dataclasses import dataclass, asdict, fields
from typing import Optional
import numpy as np
import pandas as pd
from arqManipulation import ArqManipulation as am
import os

@dataclass
class ExecutionEntity:
    execution_datetime: np.datetime64
    endpoint: Optional[str]

@dataclass
class Artifact:
    name: str
    execution_datetime: np.datetime64

@dataclass
class ArtifactInfo:
    artifact_name: str
    url: Optional[str]   #url to the object stored on the cloud, can be null
    artifact_type: str
    file_extension: str

@dataclass
class Tests:
    artifact_name: str
    name: str
    category: str
    status: str
    arguments: Optional[str]  # Sometimes details the arguments they receive 
    execution_datetime: np.datetime64

@dataclass
class ExecutionTime:
    execution_name: str  # Can be Test_Name, fixture, or function
    execution_type: str
    execution_datetime: np.datetime64
    number_runs: int
    avg_time: float
    min_time: float
    total_time: float

@dataclass
class Failures:
    artifact_name: str
    test_name: str
    execution_datetime: np.datetime64
    error: str
    details: Optional[str]  # Retrieved pytest error description

class TestData:
    def __init__(self,
        execution_entity: list[ExecutionEntity],
        artifact: list[Artifact],
        #artifact_info: list[ArtifactInfo],
        tests: list[Tests],
        execution_time: list[ExecutionTime],
        failures: list[Failures]
    ):

        self.execution_entity = self.__list_to_df__(execution_entity)
        self.artifact = self.__list_to_df__(artifact)
        self.tests = self.__list_to_df__(tests[0])
        self.execution_time = self.__list_to_df__(execution_time[0])
        self.failures = self.__list_to_df__(failures[0])

        self.load_existent()
        self.save_loaded()

    def __list_to_df__(self, input) -> list:
        if isinstance(input, list):
            return pd.DataFrame(list(map(lambda a: asdict(a), input)))
        elif isinstance(input, pd.DataFrame) and input.empty:
            return pd.DataFrame()
        return pd.DataFrame(asdict(input))

    def load_existent(self) -> None:
        """
        Load data from Parquet files for each attribute if the file exists and has data.
        Merge the loaded DataFrame with the existing DataFrame in the attribute.
        """
        for atb in vars(self):
            parquet_file_path = f"./output/{atb}.parquet"
            if os.path.exists(parquet_file_path):  # Check if the file exists
                loaded_df = am.read_parquet_file(parquet_file_path)
                if not loaded_df.empty:  # Check if the loaded DataFrame is not empty
                    existing_df = getattr(self, atb)  # 
                    merged_df = pd.concat([existing_df, loaded_df], ignore_index=True)
                    setattr(self, atb, merged_df)  
        
    def save_loaded(self):
        for atb in vars(self):
            df = getattr(self, atb)
            if isinstance(df, pd.DataFrame) and not df.empty:
                parquet_file_name = "./output/"+atb+".parquet"
                am.save_df_to_parquet(df = df, parquet_file_name=parquet_file_name)  



def get_fields(Dataclass: type) -> list[str]:
    return [field.name for field in fields(Dataclass)]

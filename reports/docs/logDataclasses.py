from dataclasses import dataclass, asdict, fields
from typing import Optional
import numpy as np
import pandas as pd
from arqManipulation import ArqManipulation as am

@dataclass
class ExecutionEntity:
    Execution_Datetime: np.datetime64
    Endpoint: Optional[str]
    Run_Time: Optional[float]

@dataclass
class Artifact:
    Name: str
    Execution_Datetime: np.datetime64

@dataclass
class ArtifactInfo:
    Artifact_Name: str
    URL: Optional[str]   #url to the object stored on the cloud, can be null
    Artifact_Type: str
    File_Extension: str

@dataclass
class Tests:
    Artifact_Name: str
    Name: str
    Category: str
    Status: str
    Arguments: Optional[str]  # Sometimes details the arguments they receive 
    Execution_Datetime: np.datetime64

@dataclass
class ExecutionTime:
    Execution_name: str  # Can be Test_Name, fixture, or function
    Execution_type: str
    Execution_Datetime: np.datetime64
    Number_Runs: int
    Avg_Time: float
    Min_Time: float
    Total_Time: float

@dataclass
class Failures:
    Artifact_Name: str
    Test_Name: str
    Execution_Datetime: np.datetime64
    Error: str
    Details: Optional[str]  # Retrieved pytest error description


class TestData:
    def __init__(self,
        execution_entity: list[ExecutionEntity],
        artifact: list[Artifact],
        #artifact_info: list[ArtifactInfo],
        tests: list[Tests],
        execution_time: list[ExecutionTime],
        failures: list[Failures]
    ):
        self.execution_entity = pd.DataFrame([asdict(execution_entity)])
        self.artifact =pd.DataFrame([asdict(artifact)])
        self.tests = pd.DataFrame(list(map(lambda t: asdict(t), tests)))
        self.execution_time = pd.DataFrame(asdict(execution_time))
        self.failures = pd.DataFrame(asdict(failures))

    def load_existent(self) -> list[pd.DataFrame]:
        """Load data from Parquet files for each attribute."""
        attributes = vars(self)  # Get instance attributes as a dictionary
        return [self.read_parquet_file(parquet_file_name=atb) for atb in attributes]


def get_fields(Dataclass: type) -> list[str]:
    return [field.name for field in fields(Dataclass)]

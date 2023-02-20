from collections import namedtuple
from datetime import datetime
from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    feature_store_file_path:str
    metadata_file_path:str
    download_dir:str

@dataclass
class DataValidationArtifact:
    accepted_file_path:str
    rejected_dir:str

@dataclass
class DataTransformationArtifact:
    transformed_train_file_path:str
    exported_pipeline_file_path:str
    transformed_test_file_path:str


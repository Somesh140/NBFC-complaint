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
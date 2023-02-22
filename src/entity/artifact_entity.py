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

class ModelTrainerArtifact:

    def __init__(self, model_trainer_ref_artifact: PartialModelTrainerRefArtifact,
                 model_trainer_train_metric_artifact: PartialModelTrainerMetricArtifact,
                 model_trainer_test_metric_artifact: PartialModelTrainerMetricArtifact
                 ):
        self.model_trainer_ref_artifact = model_trainer_ref_artifact
        self.model_trainer_train_metric_artifact = model_trainer_train_metric_artifact
        self.model_trainer_test_metric_artifact = model_trainer_test_metric_artifact

    @staticmethod
    def construct_object(**kwargs):
        model_trainer_ref_artifact=PartialModelTrainerRefArtifact(**(kwargs['model_trainer_ref_artifact']))
        model_trainer_train_metric_artifact=PartialModelTrainerMetricArtifact(**(kwargs['model_trainer_train_metric_artifact']))
        model_trainer_test_metric_artifact=PartialModelTrainerMetricArtifact(**(kwargs['model_trainer_test_metric_artifact']))
        model_trainer_artifact = ModelTrainerArtifact(model_trainer_ref_artifact,model_trainer_train_metric_artifact,model_trainer_test_metric_artifact)
        return model_trainer_artifact

    def _asdict(self):
        try:
            response = dict()
            response['model_trainer_ref_artifact'] = self.model_trainer_ref_artifact._asdict()
            response['model_trainer_train_metric_artifact'] = self.model_trainer_train_metric_artifact._asdict()
            response['model_trainer_test_metric_artifact'] = self.model_trainer_test_metric_artifact._asdict()
            return response
        except Exception as e:
            raise e
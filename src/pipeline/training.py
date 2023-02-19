from src.entity.artifact_entity import (DataIngestionArtifact)
from src.entity.config_entity import (DataIngestionConfig,TrainingPipelineConfig)
from src.component import (DataIngestion)
from src.logger import logger
from src.exception import CustomException
import sys

class TrainingPipeline:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config: TrainingPipelineConfig = training_pipeline_config

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact

        except Exception as e:
            raise CustomException(e, sys)

    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()   

        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        pass
    except Exception as e:
        logger.exception(e)
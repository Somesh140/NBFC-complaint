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

def main():
    try:
        training_pipeline_config = TrainingPipelineConfig()
        training_pipeline = TrainingPipeline(training_pipeline_config=training_pipeline_config)
        data_ingestion_artifact = training_pipeline.start_data_ingestion()
        logger.info(f"Data ingestion artifact: {data_ingestion_artifact}")
    except Exception as e:
        raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
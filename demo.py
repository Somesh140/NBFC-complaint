from src.pipeline.training import TrainingPipeline
from src.exception import CustomException
from src.logger import logger
from src.entity.config_entity import TrainingPipelineConfig
import sys

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
from src.pipeline.training import TrainingPipeline
from src.exception import CustomException
from src.logger import logger
from src.entity.config_entity import TrainingPipelineConfig
import sys

        
if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()
        training_pipeline = TrainingPipeline(training_pipeline_config=training_pipeline_config)
        training_pipeline.start()
    except Exception as e:
        raise CustomException(e, sys)
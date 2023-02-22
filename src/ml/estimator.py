import sys
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame
import shutil
import os
import time
from typing import List, Optional
import re

MODEL_SAVED_DIR="saved_models"
MODEL_NAME="finance_estimator"

class ModelResolver:

    def __init__(self,model_dir=MODEL_SAVED_DIR,model_name=MODEL_NAME):
        try:
            self.model_dir = model_dir
            self.model_name = model_name
            os.makedirs(self.model_dir,exist_ok=True)
        except Exception as e:
            raise e

    @property
    def is_model_present(self)->bool:
        if self.get_best_model_path():
            return True
        return False

    @property
    def get_save_model_path(self)->bool:
        timestamp = str(time.time())
        last_index = timestamp.find(".")
        return os.path.join(self.model_dir,f"{timestamp[:last_index]}",self.model_name)

    def get_best_model_path(self,)->Optional[str]:
        try:
            timestamps = os.listdir(self.model_dir)
            if len(timestamps)==0:
                return None
            timestamps = list(map(int,timestamps))
            latest_timestamp = max(timestamps)
            latest_model_path= os.path.join(self.model_dir,f"{latest_timestamp}",self.model_name)
            return latest_model_path
        except Exception as e:
            raise e
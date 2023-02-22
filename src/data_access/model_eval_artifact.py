from src.config import mongo_client

from src.component.model_evaluation import ModelEvaluationArtifact

class ModelEvaluationArtifactData:
    
    def __init__(self):
        self.client = mongo_client
        self.database_name="finance_artifact"
        self.collection_name = "evaluation"
        self.collection = self.client[self.database_name][self.collection_name]

    def save_eval_artifact(self, model_eval_artifact: ModelEvaluationArtifact):
        self.collection.insert_one(model_eval_artifact.to_dict())

    def get_eval_artifact(self, query):
        self.collection.find_one(query)

    
import os
import pymongo
import certifi
from src.constant import env_var


ca=certifi.where()
mongo_client=pymongo.MongoClient(env_var.mongo_db_url, tlsCAFile=ca)

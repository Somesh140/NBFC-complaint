from dataclasses import dataclass
import os

MONGO_DB_URL_ENV_KEY = "MONGO_DB_URL"


@dataclass
class EnvironmentVariable:
    mongo_db_url =os.getenv(MONGO_DB_URL_ENV_KEY)


env_var = EnvironmentVariable()
import logging
from datetime import datetime
import os
import pandas as pd
from .constant import TIMESTAMP

LOG_DIR="logs"

def get_log_file_name():
    return "log_{TIMESTAMP}.log".format(TIMESTAMP)

LOG_FILE_NAME = get_log_file_name()

if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
                    filemode="w",
                    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
                    level=logging.INFO
                    )

logger = logging.getLogger("complaintlogger")
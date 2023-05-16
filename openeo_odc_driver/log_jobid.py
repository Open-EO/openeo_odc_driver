import logging
import sys
from config import *


class LogJobID:

    def __init__(self, jID=None, file=LOG_PATH):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s "+ jID + " [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.job_id = jID
        self.logger = logging.getLogger(__name__)
    
    
    def set_job_id(self, jID):
        self.job_id = jID

    def info(self, msg):
        return self.logger.info(f"{msg}")
    
    def error(self, msg):
        return self.logger.error(f"{msg}")
    
    def debug(self, msg):
        return self.logger.debug(f"{msg}")
    
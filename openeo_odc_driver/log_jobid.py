import logging
import sys
from config import *


class LogJobID:

    def __init__(self, jID='None', file=LOG_PATH):
        self.job_id = jID
        self.file = file
        
        self.logger = logging.getLogger(__name__)
    
    
    def set_job_id(self, jID='None'):
        self.job_id = jID
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s "+ self.job_id + " [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(self.file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        

    def info(self, msg):
        return self.logger.info(f"{msg}")
    
    def error(self, msg):
        return self.logger.error(f"{msg}")
    
    def debug(self, msg):
        return self.logger.debug(f"{msg}")
    
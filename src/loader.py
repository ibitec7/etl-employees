import pandas as pd
import os
import time
import logging
import yaml

def load_config(config_file="config.yaml"):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    return config

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)

    if not os.path.exists("logs"):
        os.makedirs("logs")

    f_handler = logging.FileHandler("logs/loader.log")
    f_handler.setLevel(logging.DEBUG)

    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    c_handler.setFormatter(format)
    f_handler.setFormatter(format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return True

class Loader():
    def __init__(self, df_master, file_path):
        self.master = df_master
        self.file_path = file_path
        self.csv_path = os.path.join(file_path, "master.csv")
        self.zip_path = os.path.join(file_path, "master.zip")

    def load(self):
        start = time.time()
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)

        logging.info(f"Saving Master DataFrame as CSV file to: {self.csv_path}")
        self.master.to_csv(self.csv_path, chunksize=50000)
        logging.info("Master DataFrame saved to CSV successfully!")

        logging.info(f"Saving compressed Master DataFrame file to: {self.zip_path}")
        self.master.to_csv(self.zip_path, chunksize=50000, compression='zip')
        logging.info("Compressed Master DataFrame saved successfully!")

        end = time.time()
        elapsed = end - start

        logging.info(f"Loading of Data successful! ETA: {elapsed} seconds")

import requests
from bs4 import BeautifulSoup
import os
import zipfile
import logging
from tqdm import tqdm
import time
import yaml

def load_config(config_file="./config.yaml"):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    return config

def setup_logging(log_path):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logging.getLogger("urllib3").setLevel(logging.WARNING)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)

    if not os.path.exists(os.path.join(log_path,'extractor.log')):
        os.makedirs(log_path)

    f_handler = logging.FileHandler(os.path.join(log_path, 'extractor.log'))
    f_handler.setLevel(logging.DEBUG)

    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(format)
    f_handler.setFormatter(format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return True

class Extractor():
    def __init__(self, url, raw_path, log_path):
        try:
            assert setup_logging(log_path) == True
        except AssertionError:
            logging.warn("Logs setup has failed logs may not be recorded")
        
        self.url = url

        self.response = requests.get(self.url)
        self.soup = BeautifulSoup(self.response.content, 'html.parser')
        self.path = raw_path
        self.file_urls = set()

    def get_urls(self):
        logging.info("Collecting all file URLs...")
        links = self.soup.findAll('a',href=True)
        with tqdm(total=len(links)) as bar:
            for link in links:
                href = link['href']
                if 'blob' in href and 'employees' in href:
                    file_url = 'https://github.com' + href.replace('blob','raw')
                    self.file_urls.add(file_url)
                bar.update(1)
            bar.close()

        return True

    def download_files(self):
        logging.info("Downloading files...")
        with tqdm(total=len(self.file_urls)) as bar:
            for url in self.file_urls:
                file_name = os.path.basename(url)
                file_path = os.path.join(self.path, file_name)
                
                response = requests.get(url)

                with open(file_path, 'wb') as file:
                    file.write(response.content)

                if file_path[-4:] == '.zip':
                    with zipfile.ZipFile(file_path) as zip_file:
                        zip_file.extractall(self.path)
                bar.update(1)
            bar.close()

        logging.info("Download completed!")
        return True
    
    def extract(self):
        try:
            start = time.time()
            assert self.get_urls() == True
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            assert self.download_files() == True
            end = time.time()
            elapsed = end - start
            logging.info(f"Extraction is Successful! ETA: {elapsed} seconds")
            

        except AssertionError as e:
            logging.warn("Extraction is Unsuccessful!")

if __name__=="__main__":
    config = load_config()
    extractor = Extractor(config['urls']['data_source'],\
                         config['paths']['raw_data'], config['paths']['logs'])
    extractor.extract()    

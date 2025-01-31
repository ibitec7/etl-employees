from extractor import Extractor
from transformer import Transformer
from loader import Loader
import yaml

def load_config(config_file="config.yaml"):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    return config

if __name__=="__main__":
    config = load_config()
    pipeline = [Transformer(*config['paths']['csv_files'])]
    df_master = pipeline[0].transform()
    pipeline.append(Loader(df_master, config['paths']['transformed_data']))
    pipeline[1].load()
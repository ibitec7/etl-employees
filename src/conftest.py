import pytest
import pandas as pd
import yaml
import os

def load_config(config_file="config.yaml"):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    return config

def pytest_addoption(parser):
    parser.addoption('--url', action='store', default="https://github.com/ibitec7/DE_projects/tree/main/employees/data_employees")

@pytest.fixture
def url(request):
    return request.config.getoption('--url')

@pytest.fixture
def files():
    return ['departments.csv','dept_emp.csv','dept_manager.csv','employees.csv','salaries.zip']

@pytest.fixture
def df_dept_emp():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('dept_emp.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def df_dept_man():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('dept_manager.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def df_employees():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('employees.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def df_salaries():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('salaries.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def df_dept():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('departments.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def df_titles():
    config = load_config()
    paths = config['paths']['csv_files']
    index = [i for i,x in enumerate(paths) if x.endswith('titles.csv')]
    return pd.read_csv(paths[index[0]])

@pytest.fixture
def departments():
    return set(["Customer Service", "Development", "Finance", "Human Resources",\
                   "Marketing", "Production", "Quality Management", "Research", "Sales"])

@pytest.fixture
def df_master():
    config = load_config()
    path = config['paths']['transformed_data']
    return pd.read_csv(os.path.join(path, 'master.csv'))
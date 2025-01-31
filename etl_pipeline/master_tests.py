import pytest
import os
import yaml

def load_config(config_file="config.yaml"):
    with open(config_file,'r') as file:
        config = yaml.safe_load(file)

    return config

def test_df_master(df_master,df_salaries,df_employees):
    assert len(df_master) > len(df_salaries)
    assert df_master.emp_no.isin(df_employees.emp_no.unique()).all() == True
    assert len(df_master.dept_no.unique()) == 9
    for col in df_master.columns[1:]:
        assert df_master[col].isnull().any() == False
    assert df_master.gender.isin(["M","F"]).all() == True
    assert df_master.shape[0] == 2983774

def test_master_path():
    config = load_config()
    assert os.path.exists(os.path.join(config['paths']['transformed_data'],'master.csv')) == True
    assert os.path.exists(os.path.join(config['paths']['transformed_data'],'master.zip')) == True
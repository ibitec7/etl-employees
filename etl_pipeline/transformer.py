import pandas as pd
import numpy as np
import os
import logging
from tqdm import tqdm
import time
import yaml

def load_config(config_file="config.yaml"):
    with open(config_file,'r') as file:
        config = yaml.safe_load(file)

    return config

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)

    if not os.path.exists("logs"):
        os.makedirs("logs")

    f_handler = logging.FileHandler("logs/transformer.log")
    f_handler.setLevel(logging.DEBUG)

    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    c_handler.setFormatter(format)
    f_handler.setFormatter(format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return True

class Transformer():
    """
    Transformer class for consolidating data of employees database into a single dataframe.

    __init__:
        self.df_departments: The departments.csv dataframe

        self.dept_emp: The dept_emp.csv dataframe

        self.employees: The employees.csv dataframe
    """

    def __init__(self, dept_path, dept_emp_path, dept_man_path, emp_path, salaries_path, titles_path):
        try:
            assert setup_logging() == True
        except AssertionError as e:
            print("WARNING: Logs setup failed logs may not display properly")

        logging.info("Reading CSV files...")
        self.df_departments = pd.read_csv(dept_path)
        self.df_dept_emp = pd.read_csv(dept_emp_path)
        self.df_dept_manager = pd.read_csv(dept_man_path)
        self.df_employees = pd.read_csv(emp_path)
        self.df_salaries = pd.read_csv(salaries_path)
        self.df_titles = pd.read_csv(titles_path)
        self.df_master = pd.DataFrame({})
        logging.info("Files read successfully!")

    def prepare(self):
        logging.info("Preparing DataFrames for merges...")
        self.df_dept_emp['emp_no2'] = self.df_dept_emp['emp_no'].copy()
        self.df_dept_emp.set_index(keys='emp_no2',inplace=True)

        self.df_employees['emp_no2'] = self.df_employees['emp_no'].copy()
        self.df_employees.set_index(keys='emp_no2', inplace=True)

        self.df_salaries['emp_no2'] = self.df_salaries['emp_no'].copy()
        self.df_salaries.set_index(keys='emp_no2',inplace=True)

        self.df_titles['emp_no2'] = self.df_titles['emp_no'].copy()
        self.df_titles.set_index('emp_no2', inplace=True)

        self.df_departments['dept_no2'] = self.df_departments['dept_no'].copy()
        self.df_departments.set_index('dept_no2',inplace=True)

        logging.info("DataFrames prepared successfully!")

        return True

    def merge(self):
        logging.info("Merging DataFrames to master...")

        logging.info("Creating first-level merge DataFrames...")
        df_emp_master = self.df_employees.merge(self.df_dept_emp,how='outer')
        df_emp_master2 = self.df_employees.merge(self.df_salaries, how='outer')
        df_emp_master3 = self.df_employees.merge(self.df_titles, how='outer')

        logging.info("Creating employee and title master DataFrame...")
        df_master1 = df_emp_master.merge(df_emp_master3, how='outer')
        del(df_emp_master3)

        logging.info("Filling missing dept_no for all employees...")
        df_master2 = df_master1.copy()
        df_master2 = df_master2.iloc[:0,:]
        for dept in df_emp_master.dept_no.unique():
            emp_no  = list(df_emp_master[df_emp_master['dept_no'] == dept]['emp_no'].values)
            temp_df = df_master1[df_master1['emp_no'].isin(emp_no)]
            temp_df.loc[:, 'dept_no'] = dept
            df_master2 = pd.concat([temp_df,df_master2], axis=0)

        del(temp_df)
        del(df_master1)
        del(df_emp_master)

        df_master2 = df_master2[~df_master2['title'].isnull()]

        logging.info("Preparing for master DataFrame merge...")

        df_emp_master2['dept_no'] = None
        df_emp_master2['title'] = None

        logging.info("Merging master DataFrame...")

        df_master = pd.merge(df_emp_master2, df_master2, on='emp_no', how='left')
        df_master['to_date_x'] = df_master['to_date_x'].replace("9999-01-01",pd.Timestamp.max.date())
        df_master['to_date_y'] = df_master['to_date_y'].replace("9999-01-01",pd.Timestamp.max.date())

        df_master['to_date_x'] = pd.to_datetime(df_master['to_date_x'])
        df_master['to_date_y'] = pd.to_datetime(df_master['to_date_y'])

        df_master['from_date_x'] = pd.to_datetime(df_master['from_date_x'])
        df_master['from_date_y'] = pd.to_datetime(df_master['from_date_y'])
        del(df_master2)
        del(df_emp_master2)

        df_master['overlap'] = (
            (df_master['from_date_x'] >= df_master['from_date_y']) &
            (df_master['from_date_x'] <= df_master['to_date_y']) &
            (df_master['to_date_x'] >= df_master['from_date_y']) &
            (df_master['to_date_x'] <= df_master['to_date_y'])
        )

        def process1():
            total_chunks = 1000
            chunk_size = round(len(df_master) / total_chunks)

            with tqdm(total=total_chunks) as bar:
                start = 0
                for _ in range(total_chunks):
                    end = min(start + chunk_size, len(df_master))
                    df_master.loc[start:end,'dept_no'] = df_master.loc[start:end]\
                        .apply(lambda row: row['dept_no_y'] if row['overlap'] else row['dept_no_x'], axis=1)
                    start = end
                    bar.update(1)
                bar.close()
        
        def process2():
            total_chunks = 1000
            chunk_size = round(len(df_master) / total_chunks)

            with tqdm(total=total_chunks) as bar2:
                start = 0
                for _ in range(total_chunks):
                    end = min(start + chunk_size, len(df_master))
                    df_master.loc[start:end,'title'] = df_master.loc[start:end]\
                        .apply(lambda row: row['title_y'] if row['overlap'] else row['title_x'], axis=1)
                    start = end
                    bar2.update(1)
                bar2.close()

        logging.info("Filling missing dept_no for master DataFrame...")
        process1()

        logging.info("Filling missing title for master DataFrame...")
        process2()
        
        self.df_master = df_master

        logging.info("Master DataFrame created successfully!")

        return True

    def clean(self):
        logging.info("Cleaning master DataFrame...")
        logging.info("Dropping columns...")
        drop_cols = [col for col in self.df_master.columns if col.endswith('_y')]
        drop_cols.extend(['overlap','title_x','dept_no_x'])
        self.df_master.drop(drop_cols, inplace=True, axis=1)
        logging.info("Renaming columns...")
        rename_cols = [col for col in self.df_master.columns if col.endswith('_x')]
        toname_cols = [col.replace('_x','') for col in rename_cols]
        self.df_master.rename(axis=1, mapper=dict(zip(rename_cols,toname_cols)),inplace=True)
        self.df_master = self.df_master[self.df_master.dept_no.notnull() & self.df_master.title.notnull()]
        self.df_master['dept_name'] = None
        df_master2 = self.df_master.copy()
        df_master2 = df_master2.iloc[:0,:]

        logging.info("Filling in missing department names...")
        for dept in self.df_departments.dept_no.unique():
            name = self.df_departments[self.df_departments.dept_no == dept]['dept_name'].values[0]
            temp_df = self.df_master[self.df_master['dept_no'] == dept].copy()
            temp_df.loc[:, 'dept_name'] = name
            df_master2 = pd.concat([temp_df,df_master2], axis=0)

        self.df_master = df_master2.copy()
        del(df_master2)
        del(temp_df)
        self.df_master.reset_index(drop=True, inplace=True)
        logging.info("Master DataFrame cleaned successfully!")
        return True

    def transform(self):
        try:
            start = time.time()
            assert self.prepare() == True
            assert self.merge() == True
            assert self.clean() == True
            end = time.time()
            elapsed = end - start
            logging.info(f"Transformation of data successful! ETA: {elapsed} seconds")
            return self.df_master

        except AssertionError as e:
            logging.warn("Transformation of data failed!")
            return
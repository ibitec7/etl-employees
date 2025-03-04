**This repository is migrated from a previous repository named DE_projects**

This project is an ETL pipeline for the MySQL employees database. This pipeline takes data from all tables of the 
database and denormalizes it to one master dataframe. The data in the database is at 3NF standard. The consolidated
master dataframe brings this down to a 1NF.

## The master dataframe follows 1NF because
- Atomicity: Data in each column is atomic as it can not be divided further examples of non-atomic data can be lists.
- Uniqueness: Each record is unique (there are no duplicate entries) and the index is the primary key.
- Consistency: Data in each column has the same data type.

To run the container you may clone this directory and either run the pipeline with Docker (recommended) or
run the pipeline manually. The steps for each is listed below:

## Cloning the directory
- Make an empty folder with any name <dir_name>
- cd <dir_name>
- git init
- git remote add origin https://github.com/ibitec7/DE_projects.git
- git sparse-checkout init --cone
- git sparse-checkout set employees/etl_pipeline
- git pull origin main

## Running with Docker (recommended)
- cd employees/etl_pipeline
- docker build -t <image_name> .
- docker run --name <container_name> -it <image_name>
- docker cp <container_name>:/app/master_data </path/to/your_directory>

## Running manually
- cd employees/etl_pipeline
- bash run_pipeline.sh
- cp master_data </path/to/your_directory>
  

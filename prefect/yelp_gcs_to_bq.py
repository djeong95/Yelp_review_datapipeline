import os
import json
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
"""
This script uses prefect to send JSON files from Google Cloud Storage (Dake Lake) to BigQuery (Data Warehouse).
There is basic data cleaning and transformation done while data transfer to Data Warehouse occurs.

Table names created should be documented in terraform to create appropriate tables in advance.

"""

# extract paths for data from GCS to be put into BQ; credentials might be needed
@task(retries=3)
def extract_from_gcs(term:str, location:str, index:int) -> Path:
    """Download JSON files from GCS
    
    :param term: The term corresponding to the data (ex. ['Restaurants', 'Food', 'Coffee & Tea']).
    :param location: The name of location corresponding to the data (ex. Torrance).
    :param index: The index of the location.
    :return: The path of the written JSON file.
    """
    gcs_path = f"data/{term}-{location}-{index}.json"
    gcs_block = GcsBucket.load("yelp-data-lake-yelp-pipeline-project")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True)
def fetch_location_df(filename):
    """
    Read a stored CSV file with Latitude and Longitude data for ~470 locations in California.
    
    :param filename: The name of the CSV file to read.
    :return: A DataFrame with the location data.
    """
    try:
        df_locations = pd.read_csv(filename) #"california_lat_long_cities.csv"
        # Convert all columns of df_locations to their equivalent Python types. 
        # Prefect would not accept float64 that is non native Python floats
        df_locations = df_locations.astype('object')
    except FileNotFoundError:
        print("CSV File not found.")
    return df_locations

# use pandas to perform basic data cleaning
@task()
def read_json_transform_df(path:Path) -> pd.DataFrame:
    """
    Data cleaning
    """
    df = pd.read_json(path)
    
    return df

@task()
def write_bq(df:pd.DataFrame, term:str) -> None:
    """
    Write DataFrame to BigQuery
    """
    
    gcp_credentials_block = GcpCredentials.load("yelp-gcs-creds")
    df.to_gbq(
        destination_table=f"yelp_data_raw.{term}_data_raw",
        project_id="yelp-pipeline-project",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append"
    )
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq(terms: list, start_slice: int, end_slice: int):
    """
    
    """
    total_rows = 0
    df_locations = fetch_location_df("california_lat_long_cities.csv")
    for term in terms: 
        for i in range(start_slice, end_slice):
            print(term, df_locations.iloc[i]['Name'], i)
    
            path = extract_from_gcs(term, df_locations.iloc[i]['Name'], i)
            df = read_json_transform_df(path)
            row_count = write_bq(df, term)
                
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
    
            print(df.head(20))
            total_rows += row_count

if __name__ == "__main__":
    TERMS = ['Restaurants'] # ['Juice Bars & Smoothies', 'Desserts', 'Bakeries', 'Coffee & Tea', 'Bubble Tea'] ['Restaurants', 'Food']
    START_SLICE = 0
    END_SLICE = 1
    
    etl_gcs_to_bq(TERMS, START_SLICE, END_SLICE)
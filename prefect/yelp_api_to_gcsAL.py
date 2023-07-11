import os
import pandas as pd
import requests
import json
import numpy as np
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3)
def fetch_location_df(filename):
    # Read csv file that has Lat / Long of 470 areas in CA
    try:
        df_locations = pd.read_csv(filename) #"california_lat_long_cities.csv"
        # Convert all columns of df_locations to their equivalent Python types. Prefect would not accept float64 that is
        # non native Python floats
        df_locations = df_locations.astype('object')
    except FileNotFoundError:
        print("CSV File not found.")
    return df_locations

@task()
def write_local(data: json, term:str, location:str, index:int) -> Path:
    """Write DataFrame out locally as json file"""
    path = Path(f"data/{term}-{location}-{index}.json")
    # Write the data to the JSON file # 'yelp_data_torr.json'
    with open(path, 'w') as f:
        json.dump(data, f)
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("yelp-data-lake-yelp-pipeline-project")
    gcs_block.upload_from_path(
        from_path = f"{path}",
        to_path = path,
        timeout=120 )
    return

#@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
@task(log_prints=True, retries=3)
def get_api_data(url, headers, term, lat, long, limit = 50):
    """
    Retrieve data from Yelp Fusion API using the provided URL, headers, and parameters,
    and return the JSON data.
    :param url: API Host and search path (ie 'https://api.yelp.com/v3/businesses/search')
    :param headers: a dictionary of headers to send with API requests
    :param term: A term to include in the API query (ie 'Food', 'Restaurants', 'Coffee & Tea')
    :param lat: A float64 containing df_location's Latitude
    :param long: A float64 containing df_location's Longitude
    :return: data
    """
    # Initialize variables
    offset = 0
    data = []
    # Retrieve data from the Yelp API using pagination
    while True:

        # Set the query parameters for this location
        parameters = {
            'limit': 50,
            'term': term,
            'is_closed': False,
            'latitude': float(lat),
            'longitude': float(long),
            'radius': 10000,
            'offset': offset
        }
        # Make API Request
        response = requests.get(url, headers=headers, params=parameters)

        # Check for errors and break out of the loop if there are no more results
        response.raise_for_status()
        business = response.json().get('businesses', [])
        if not business:
            break

        # Append the retrieved data to the overall data list and increment the offset
        data.extend(business)
        offset += limit

        # This endpoint returns up to 1000 businesses
        if offset == 1000:
            break

    return data

@flow(name="Subflow", log_prints=True)
def pull_data_across_locations(url, headers, terms, df_locations):
    """
    Tap into get_api_data function to retrieve data from Yelp Fusion API
    using the provided URL, headers, and parameters, and return the JSON data.
    :param url: API Host and search path (ie 'https://api.yelp.com/v3/businesses/search')
    :param headers: a dictionary of headers to send with API requests
    :param terms: A list of terms to include in the API query (ie ['Food', 'Restaurants', 'Coffee & Tea'])
    :param df_locations: A dataframe containing location data like place name, Latitude, and Longitude
    :return: data
    """
    results = []

    for term in terms:

        for i in range(df_locations.shape[0]):
            print(term, df_locations.iloc[i]['Name'], i)
            # Get results from get_api_data
            result = get_api_data(url, headers, term, df_locations.iloc[i]['Latitude'],
                                  df_locations.iloc[i]['Longitude'])
            path = write_local(result, term, df_locations.iloc[i]['Name'], i)
            write_gcs(path)
            
            # Write into MySQL Workbench Server; all db_variables are global except result
            # insert_data_to_db(result, db_HOST, db_USER, db_PASSWORD, db_DATABASE, db_TABLE_NAME)
            results.extend(result)

    return results

@flow(name="Ingest Flow")
def etl_api_to_gcs() -> None:
    
    df_locations = fetch_location_df("california_lat_long_cities.csv")
    # df_county = fetch_location_df("california_county_cities.csv")

    # print(df_locations[233::])
    
    # Assign url and api_key for Yelp Fusion API
    URL = 'http://api.yelp.com/v3/businesses/search'
    API_KEY = os.getenv("YELP_API_KEY")  # your api key
    HEADERS = {'Authorization': 'Bearer %s' % API_KEY}
    TERMS = ['Restaurants'] # ['Juice Bars & Smoothies', 'Desserts', 'Bakeries', 'Coffee & Tea', 'Bubble Tea']
    
    pull_data_across_locations(URL, HEADERS, TERMS, df_locations[::233]) # df_locations[::233] is AtoL; df_locations[233::] is MtoZ
    
    # send local csv lat long file to GCS
    write_gcs("california_lat_long_cities.csv")
    # send local csv counties cities file to GCS
    write_gcs("california_county_cities.csv")

    # print(api_results)
    # ['Restaurants', 'Food']
    # Specify parameters for API
    # Torrance,33.83585,-118.340628 / iloc[415:416]

if __name__ == "__main__":
    etl_api_to_gcs()



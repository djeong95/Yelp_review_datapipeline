"""
Data extraction from Yelp Fusion API.

First part of the script is meant to use the Yelp Fusion API by using the Search API to query for businesses
by a list of search terms and locations.

Please refer to https://docs.developer.yelp.com/docs/get-started for the API
documentation.

Second part of the script is meant to store the queried data into a db format
"""
import mysql.connector
import requests
import time
import json
import pandas as pd

start_time = time.time()

# try:
#     with open('yelp_data_torr.json', 'r') as f:
#         existing_data = json.load(f)
# except FileNotFoundError:
#     existing_data = {'businesses': []}
#     with open('yelp_data_torr.json', 'w') as f:
#         json.dump(existing_data, f)

# Read csv file that has Lat / Long of 470 areas in CA
try:
    df_locations = pd.read_csv("cal_cities_lat_long.csv")
except FileNotFoundError:
    print("CSV File not found.")


# Assign url and api_key for Yelp Fusion API
URL = 'https://api.yelp.com/v3/businesses/search'

API_KEY =   # your api key

HEADERS = {
    'Authorization': 'Bearer %s' % API_KEY
}

TERMS = ['Restaurants', 'Food']

# 'term': 'Restaurants', 'Food'
# 'Juice Bars & Smoothies', 'Desserts', 'Bakeries', 'Coffee & Tea', 'Bubble Tea'

# Specify parameters for API
# Torrance,33.83585,-118.340628 / iloc[415:416]

db_HOST = '127.0.0.1'
db_USER = 'root'
db_PASSWORD = 'F@1thAlone!'
db_DATABASE = 'yelpdb'
db_TABLE_NAME = "yelp_data"


def get_api_data(url, headers, term, lat, long, limit = 50):
    """
    Retrieve data from Yelp Fusion API using the provided URL, headers, and parameters,
    and return the JSON data.
    :param url: API Host and search path (ie 'https://api.yelp.com/v3/businesses/search')
    :param headers: a dictionary of headers to send with API requests
    :param terms: A list of terms to include in the API query
    :param df_locations: A dataframe containing location data like place name, Latitude, and Longitude
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
            'latitude': lat,
            'longitude': long,
            'radius': 25000,
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


def pull_data_across_locations(url, headers, terms, df_locations):

    results = []
    for term in terms:

        for i in range(df_locations.shape[0]):

            result = get_api_data(url, headers, term, df_locations.iloc[i]['Latitude'],
                                  df_locations.iloc[i]['Longitude'])
            results.extend(result)

    return results


def insert_data_to_db(data, host, user, password, database, tablename):
    try:
        # Connect to the MySQL database called yelpdb
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Insert the data into the MySQL table
        mycursor = mydb.cursor()

        sql = "INSERT INTO {} (id, alias, name, image_url,\
        is_closed, url, review_count, categories, rating, coordinates, transactions,\
        price, location, phone, display_phone, distance)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tablename)

        for item in data:

            # Convert the categories list to a JSON formatted string
            price = item.get('price', None)
            val = (item['id'], item['alias'], item['name'], item['image_url'], item['is_closed'], item['url'],
                   item['review_count'], json.dumps(item['categories']), item['rating'], json.dumps(item['coordinates']),
                   json.dumps(item['transactions']),
                   price, json.dumps(item['location']), item['phone'], item['display_phone'], item['distance'])
            mycursor.execute(sql, val)
        mydb.commit()
        print(mycursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table: {}".format(error))
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed.")


test_result = pull_data_across_locations(URL, HEADERS, TERMS, df_locations.iloc[415:416])
insert_data_to_db(test_result, db_HOST, db_USER, db_PASSWORD, db_DATABASE, db_TABLE_NAME)

# import mysql.connector
# # Connect to the MySQL server
# mydb = mysql.connector.connect(
#     host='127.0.0.1',
#     user='root',
#     password='F@1thAlone!',
#     database='yelpdb'
# )
#
# # Select all rows from the new table
# mycursor = mydb.cursor()
# mycursor.execute("SELECT * FROM yelp_data")
#
# # Print the results
# for row in mycursor.fetchall():
#     print(row)

print("Code running completed.")
print ("My program took", time.time() - start_time, "to run")

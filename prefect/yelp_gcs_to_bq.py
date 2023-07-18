import os
import io
import json
import time
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

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

@task()
def write_local(data:json, term:str, location:str, index:int) -> Path:
    """Write DataFrame out locally as json file

    :param term: The term corresponding to the data (ex. ['Restaurants', 'Food', 'Coffee & Tea']).
    :param location: The name of location corresponding to the data (ex. Torrance).
    :param index: The index of the location.
    :return: The path of the written JSON file.
    """
    path = Path(f"transformed_data/transformed_{term}-{location}-{index}.json")
    
    # Write the data to the JSON file # 'yelp_data_torr.json'
    with open(path, 'w') as f:
        f.write(data)
    
    return path

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def geolocate_with_address(row):
    geolocator = Nominatim(user_agent='my-applications', timeout=10)

    time.sleep(1)
    if pd.isna(row['latitude']) and pd.isna(row['longitude']):
        location = geolocator.geocode(row['address'])
        if location:
            row['latitude'] = location.latitude
            row['longitude'] = location.longitude
    return row


@flow(log_prints=True)
def read_json_transform_df(path:Path, set_cities:set) -> pd.DataFrame:
    """
    Data cleaning and transformation before putting it into BigQuery.
    Section 1. Clean categories
    1. convert categories from string to python dictionary
    2. clean categories to have only alias in a list and get rid of non-related categories
    3. 

    Section 2. Clean coordinates
    Section 3. Clean coordinates and locations


    :param path: The path url of Google Cloud Storage to transform from JSON file to pd.DataFrame
    """

    # Below sets were obtained after EDA into categories.
    # NONFOOD_CATEGORIES and DEFINITELY_NO_CATEGORIES need to be cleaned out since they are nonrelated categories
    NONFOOD_CATEGORIES = {'copyshops', 'bikes', 'seafoodmarkets', 'cheese','importedfood','buildingsupplies', 'pumpkinpatches', 'hardware', 'vitaminssupplements', 'cannabisdispensaries', 'artclasses', 'horse_boarding', 'pilates', 'sewingalterations', 'reiki', 'musicians', 'educationservices', 'servicestations', 'skishops',  'marketing', 'fleamarkets', 'spiritual_shop', 'tiling', 'carwash', 'selfstorage',  'hiking', 'accessories', 'alternativemedicine', 'localservices', 'convenience', 'indoor_playcenter', 'saunas', 'grocery', 'shipping_centers', 'playgrounds', 'markets', 'weddingchappels', 'bouncehouserentals', 'tabletopgames', 'doulas', 'photographystores', 'tcm', 'icedelivery', 'pettingzoos', 'wholesalers', 'pianobars', 'surfshop', 'couriers', 'homecleaning', 'foodbanks', 'acupuncture', 'personal_shopping', 'computers', 'oilchange', 'boattours', 'escapegames', 'shavedice', 'brasseries', 'custommerchandise', 'sportswear', 'drivethrubars', 'buddhist_temples', 'weightlosscenters', 'balloonservices', 'registrationservices', 'catering', 'usedbooks', 'karaoke', 'thrift_stores', 'vintage', 'autocustomization', 'souvenirs', 'brewingsupplies', 'parking', 'vocation', 'beautysvc', 'tastingclasses', 'hottubandpool', 'flowers', 'distilleries', 'csa', 'ranches', 'eventservices', 'paintball', 'interiordesign', 'wigs', 'magicians', 'appliances', 'limos', 'plumbing', 'irrigation', 'spraytanning', 'organic_stores', 'candlestores', 'airport_shuttles', 'fishing', 'dog_parks', 'horseracing', 'bingo', 'cookingclasses', 'bike_repair_maintenance', 'animalshelters', 'homedecor', 'gardens', 'tours', 'womenscloth', 'laboratorytesting', 'firewood', 'paydayloans', 'eventplanning', 'headshops', 'hobbyshops', 'publicmarkets', 'bocceball', 'gyms', 'grillservices', 'cardioclasses', 'countryclubs', 'reststops', 'nonprofit', 'shoppingcenters', 'gamemeat', 'religiousitems', 'electronics', 'eatertainment', 'aerialfitness', 'pest_control', 'florists', 'stadiumsarenas', 'kitchenincubators', 'costumes', 'waterpurification', 'hookah_bars', 'djs', 'football', 'shopping', 'physicaltherapy', 'cafeteria', 'barcrawl', 'amateursportsteams', 'suppliesrestaurant', 'bikerentals', 'campgrounds', 'tobaccoshops', 'boating', 'beaches', 'farms', 'vapeshops', 'countrydancehalls', 'gardening', 'cyclingclasses', 'partyequipmentrentals', 'pet_sitting', 'gemstonesandminerals', 'religiousorgs', 'cigarbars', 'elementaryschools', 'supperclubs', 'watersuppliers', 'culturalcenter', 'hotels', 'travelservices', 'mini_golf', 'evchargingstations', 'childcloth', 'artschools', 'outdoorgear', 'vinyl_records', 'reflexology', 'waterdelivery', 'international', 'butcher', 'fitness', 'laundryservices', 'videoandgames', 'paintandsip', 'visitorcenters', 'attractionfarms', 'bootcamps', 'farmersmarket', 'beachequipmentrental', 'trains', 'landmarks', 'golf', 'resorts', 'petadoption', 'winetasteclasses', 'partycharacters', 'mobilephones', 'outlet_stores', 'boatcharters', 'groomer', 'itservices', 'petboarding', 'homehealthcare', 'customcakes', 'chiropractors', 'sommelierservices', 'healthcoach', 'nutritionists', 'artmuseums', 'popupshops', 'museums', 'service_stations', 'fireworks', 'wholesale_stores', 'battingcages', 'fueldocks', 'antiques', 'homeandgarden', 'artsandcrafts', 'galleries', 'livestocksupply', 'surfing', 'personalchefs', 'vacation_rentals', 'amusementparks', 'hats', 'bodyshops', 'cosmetics', 'casinos', 'hospitals', 'eyebrowservices', 'postoffices', 'musicvenues', 'giftshops', 'tennis', 'guesthouses', 'opticians', 'swimmingpools', 'yoga', 'stationery', 'christmastrees', 'beer_and_wine', 'cookingschools', 'bedbreakfast', 'rvparks', 'rugs', 'autorepair', 'autopartssupplies', 'discountstore', 'tradclothing', 'discgolf', 'fashion', 'foodtours', 'recreation', 'pharmacy', 'petstore', 'meats', 'paddleboarding', 'hotelstravel', 'kiteboarding', 'psychic_astrology', 'parks', 'internetcafe', 'specialtyschools', 'kitchensupplies', 'lingerie', 'bookstores', 'arcades', 'lancenters', 'toys', 'threadingservices', 'lifecoach', 'herbsandspices', 'publicservicesgovt', 'auto', 'marinas', 'taxis', 'huntingfishingsupplies', 'health', 'medcenters', 'kitchenandbath', 'kids_activities', 'dancestudio', 'massage_therapy', 'pet_training', 'oliveoil', 'axethrowing', 'bowling', 'barbers', 'kiosk', 'intlgrocery', 'localflavor', 'bartenders', 'petbreeders', 'laundromat', 'musicvideo', 'arts', 'yelpevents', 'candy', 'watches', 'churches', 'waterstores', 'winetours', 'herbalshops', 'popcorn', 'deptstores', 'medicalspa', 'pickyourown', 'jewelry', 'fabricstores', 'truckrepair', 'movietheaters', 'guns_and_ammo', 'education', 'advertising', 'sportgoods', 'partysupplies', 'floraldesigners', 'skincare', 'popuprestaurants', 'horsebackriding', 'healthtrainers', 'poolhalls', 'mags', 'homeappliancerepair', 'golflessons', 'beverage_stores', 'smog_check_stations', 'businessconsulting', 'beertours', 'propane', 'nikkei', 'walkingtours', 'cosmeticdentists', 'artspacerentals', 'sharedofficespaces', 'fooddeliveryservices', 'menscloth', 'clothingrental', 'venues', 'shoes', 'nightlife', 'massage', 'drugstores', 'cabaret', 'lounges', 'active', 'healthmarkets', 'specialed', 'photoboothrentals', 'spas', 'wedding_planning', 'teambuilding', 'festivals', 'naturopathic', 'comedyclubs', 'social_clubs', 'danceclubs', 'jazzandblues', 'wildlifecontrol', 'outdoormovies', 'furniture', 'mobilephonerepair', 'internalmed', 'meditationcenters', 'theater', 'virtualrealitycenters'}
    DEFINITELY_NO_CATEGORIES = {'grocery', 'convenience', 'drugstores'}
    # All categories in the dataset
    ALL_CATEGORIES = {'delis', 'copyshops', 'japanese', 'bikes', 'buildingsupplies', 'pumpkinpatches', 'hardware', 'greek', 'honey', 'winetastingroom', 'vitaminssupplements', 'cannabisdispensaries', 'acaibowls', 'artclasses', 'horse_boarding', 'diners', 'foodtrucks', 'pilates', 'sewingalterations', 'reiki', 'musicians', 'educationservices', 'servicestations', 'skishops', 'bistros', 'mideastern', 'raw_food', 'marketing', 'fleamarkets', 'cheesesteaks', 'hotpot', 'uzbek', 'spiritual_shop', 'tiling', 'breweries', 'diyfood', 'carwash', 'selfstorage', 'southern', 'arabian', 'coffee', 'eritrean', 'hiking', 'fishnchips', 'accessories', 'alternativemedicine', 'kombucha', 'localservices', 'convenience', 'indoor_playcenter', 'sushi', 'pubs', 'irish', 'oaxacan', 'cantonese', 'japacurry', 'saunas', 'grocery', 'colombian', 'szechuan', 'shipping_centers', 'izakaya', 'playgrounds', 'markets', 'weddingchappels', 'bouncehouserentals', 'tabletopgames', 'cocktailbars', 'doulas', 'photographystores', 'chocolate', 'tcm', 'dinnertheater', 'icedelivery', 'food', 'pettingzoos', 'puertorican', 'halal', 'wholesalers', 'pianobars', 'surfshop', 'laotian', 'couriers', 'homecleaning', 'foodbanks', 'armenian', 'divebars', 'acupuncture', 'basque', 'austrian', 'personal_shopping', 'computers', 'korean', 'oilchange', 'boattours', 'escapegames', 'shavedice', 'brasseries', 'custommerchandise', 'falafel', 'sportswear', 'drivethrubars', 'buddhist_temples', 'bangladeshi', 'poutineries', 'weightlosscenters', 'balloonservices', 'somali', 'gourmet', 'registrationservices', 'catering', 'streetvendors', 'usedbooks', 'karaoke', 'thrift_stores', 'themedcafes', 'vintage', 'autocustomization', 'souvenirs', 'brewingsupplies', 'italian', 'bulgarian', 'parking', 'yucatan', 'russian', 'cakeshop', 'vocation', 'beautysvc', 'tastingclasses', 'hottubandpool', 'flowers', 'distilleries', 'csa', 'ranches', 'teppanyaki', 'cupcakes', 'eventservices', 'paintball', 'interiordesign', 'dominican', 'latin', 'wigs', 'magicians', 'sardinian', 'appliances', 'limos', 'plumbing', 'irrigation', 'churros', 'spraytanning', 'filipino', 'lebanese', 'organic_stores', 'candlestores', 'airport_shuttles', 'coffeeroasteries', 'fishing', 'asianfusion', 'newmexican', 'dog_parks', 'horseracing', 'bingo', 'cookingclasses', 'bike_repair_maintenance', 'animalshelters', 'champagne_bars', 'homedecor', 'senegalese', 'ukrainian', 'gardens', 'tours', 'womenscloth', 'laboratorytesting', 'firewood', 'paydayloans', 'eventplanning', 'headshops', 'noodles', 'hobbyshops', 'publicmarkets', 'wine_bars', 'sicilian', 'tea', 'bocceball', 'gyms', 'juicebars', 'grillservices', 'cardioclasses', 'comfortfood', 'soup', 'wraps', 'countryclubs', 'reststops', 'nonprofit', 'australian', 'restaurants', 'vermouthbars', 'shoppingcenters', 'gastropubs', 'gamemeat', 'bakeries', 'bagels', 'religiousitems', 'desserts', 'electronics', 'eatertainment', 'aerialfitness', 'pest_control', 'florists', 'chicken_wings', 'stadiumsarenas', 'seafood', 'kitchenincubators', 'costumes', 'waterpurification', 'hookah_bars', 'djs', 'football', 'shopping', 'physicaltherapy', 'cafeteria', 'rotisserie_chicken', 'cideries', 'barcrawl', 'amateursportsteams', 'suppliesrestaurant', 'bikerentals', 'campgrounds', 'vietnamese', 'tobaccoshops', 'boating', 'gaybars', 'polynesian', 'beaches', 'pancakes', 'farms', 'vapeshops', 'countrydancehalls', 'gardening', 'cyclingclasses', 'partyequipmentrentals', 'pet_sitting', 'gemstonesandminerals', 'religiousorgs', 'cigarbars', 'elementaryschools', 'supperclubs', 'watersuppliers', 'georgian', 'macarons', 'culturalcenter', 'southafrican', 'hotels', 'travelservices', 'mini_golf', 'evchargingstations', 'hkcafe', 'childcloth', 'artschools', 'outdoorgear', 'vinyl_records', 'reflexology', 'waterdelivery', 'international', 'butcher', 'pakistani', 'fitness', 'piadina', 'mexican', 'tamales', 'brewpubs', 'laundryservices', 'poke', 'videoandgames', 'empanadas', 'paintandsip', 'visitorcenters', 'attractionfarms', 'buffets', 'chickenshop', 'peruvian', 'tradamerican', 'bootcamps', 'mongolian', 'farmersmarket', 'beachequipmentrental', 'trains', 'landmarks', 'golf', 'resorts', 'portuguese', 'tikibars', 'petadoption', 'winetasteclasses', 'partycharacters', 'shavedsnow', 'pastashops', 'gelato', 'burmese', 'mobilephones', 'outlet_stores', 'boatcharters', 'gluten_free', 'chimneycakes', 'moroccan', 'cajun', 'breakfast_brunch', 'speakeasies', 'hainan', 'groomer', 'itservices', 'petboarding', 'homehealthcare', 'customcakes', 'brazilian', 'caribbean', 'chiropractors', 'sommelierservices', 'healthcoach', 'nutritionists', 'artmuseums', 'popupshops', 'museums', 'honduran', 'service_stations', 'fireworks', 'himalayan', 'wholesale_stores', 'venezuelan', 'battingcages', 'fueldocks', 'kosher', 'antiques', 'foodstands', 'bubbletea', 'scottish', 'homeandgarden', 'artsandcrafts', 'galleries', 'livestocksupply', 'surfing', 'pretzels', 'personalchefs', 'hotdogs', 'donuts', 'vacation_rentals', 'amusementparks', 'hats', 'bodyshops', 'cosmetics', 'casinos', 'northernmexican', 'hospitals', 'eyebrowservices', 'vegetarian', 'postoffices', 'musicvenues', 'giftshops', 'cheese', 'calabrian', 'tennis', 'guesthouses', 'opticians', 'swimmingpools', 'yoga', 'stationery', 'tuscan', 'singaporean', 'cuban', 'christmastrees', 'beer_and_wine', 'cookingschools', 'bedbreakfast', 'chinese', 'rvparks', 'rugs', 'autorepair', 'autopartssupplies', 'discountstore', 'tradclothing', 'discgolf', 'fashion', 'foodtours', 'recreation', 'pharmacy', 'petstore', 'hungarian', 'srilankan', 'panasian', 'food_court', 'meats', 'paddleboarding', 'syrian', 'hotelstravel', 'kiteboarding', 'sandwiches', 'seafoodmarkets', 'soulfood', 'psychic_astrology', 'conveyorsushi', 'parks', 'internetcafe', 'specialtyschools', 'kitchensupplies', 'lingerie', 'bookstores', 'arcades', 'lancenters', 'toys', 'kebab', 'threadingservices', 'lifecoach', 'herbsandspices', 'publicservicesgovt', 'auto', 'cafes', 'afghani', 'marinas', 'taxis', 'huntingfishingsupplies', 'fondue', 'argentine', 'health', 'sportsbars', 'spanish', 'modern_european', 'medcenters', 'kitchenandbath', 'kids_activities', 'dancestudio', 'massage_therapy', 'tex-mex', 'pet_training', 'slovakian', 'importedfood', 'oliveoil', 'burgers', 'axethrowing', 'bowling', 'barbers', 'kiosk', 'intlgrocery', 'cambodian', 'localflavor', 'bartenders', 'petbreeders', 'laundromat', 'musicvideo', 'arts', 'yelpevents', 'candy', 'thai', 'watches', 'churches', 'belgian', 'waterstores', 'winetours', 'herbalshops', 'czech', 'popcorn', 'deptstores', 'jaliscan', 'medicalspa', 'taiwanese', 'meaderies', 'pickyourown', 'jewelry', 'icecream', 'fabricstores', 'creperies', 'truckrepair', 'newamerican', 'scandinavian', 'salad', 'movietheaters', 'malaysian', 'guns_and_ammo', 'education', 'advertising', 'sportgoods', 'partysupplies', 'floraldesigners', 'skincare', 'african', 'popuprestaurants', 'horsebackriding', 'mediterranean', 'vegan', 'healthtrainers', 'poolhalls', 'french', 'guamanian', 'beerbar', 'mags', 'beergardens', 'homeappliancerepair', 'golflessons', 'beverage_stores', 'smog_check_stations', 'shanghainese', 'businessconsulting', 'beertours', 'propane', 'smokehouse', 'tapas', 'nikkei', 'british', 'walkingtours', 'indonesian', 'cosmeticdentists', 'artspacerentals', 'wineries', 'sharedofficespaces', 'irish_pubs', 'fooddeliveryservices', 'menscloth', 'clothingrental', 'venues', 'shoes', 'nightlife', 'massage', 'bars', 'drugstores', 'trinidadian', 'cabaret', 'lounges', 'iberian', 'ramen', 'salvadoran', 'steak', 'dimsum', 'indpak', 'pizza', 'waffles', 'active', 'healthmarkets', 'ethiopian', 'persian', 'specialed', 'photoboothrentals', 'spas', 'wedding_planning', 'teambuilding', 'tapasmallplates', 'polish', 'hotdog', 'nicaraguan', 'festivals', 'newcanadian', 'hawaiian', 'naturopathic', 'comedyclubs', 'haitian', 'social_clubs', 'danceclubs', 'whiskeybars', 'german', 'jazzandblues', 'wildlifecontrol', 'outdoormovies', 'furniture', 'mobilephonerepair', 'internalmed', 'bbq', 'meditationcenters', 'tacos', 'theater', 'egyptian', 'virtualrealitycenters', 'turkish'}
    
    # Food and Bars categories - this includes bars that serve food or restaurants that serve drinks
    FOOD_AND_BARS_CATEGORIES = {'poutineries', 'sardinian', 'bars', 'divebars', 'tamales', 'winetastingroom', 'comfortfood', 'somali', 'rotisserie_chicken', 'cuban', 'puertorican', 'pastashops', 'meaderies', 'whiskeybars', 'indpak', 'vermouthbars', 'moroccan', 'cupcakes', 'newmexican', 'polynesian', 'falafel', 'pancakes', 'yucatan', 'bbq', 'oaxacan', 'ukrainian', 'arabian', 'foodstands', 'brazilian', 'wineries', 'japacurry', 'pubs', 'himalayan', 'newcanadian', 'tacos', 'australian', 'hainan', 'dinnertheater', 'foodtrucks', 'acaibowls', 'georgian', 'sicilian', 'calabrian', 'irish_pubs', 'dimsum', 'kebab', 'teppanyaki', 'colombian', 'newamerican', 'chinese', 'vietnamese', 'delis', 'irish', 'eritrean', 'southern', 'wraps', 'senegalese', 'tapasmallplates', 'hawaiian', 'guamanian', 'mediterranean', 'hotdog', 'vegan', 'laotian', 'panasian', 'polish', 'beerbar', 'argentine', 'steak', 'sandwiches', 'tex-mex', 'bagels', 'german', 'salad', 'creperies', 'gluten_free', 'pretzels', 'poke', 'jaliscan', 'noodles', 'buffets', 'basque', 'iberian', 'chickenshop', 'singaporean', 'modern_european', 'desserts', 'donuts', 'food_court', 'breweries', 'mexican', 'sushi', 'dominican', 'salvadoran', 'restaurants', 'hotdogs', 'gelato', 'kosher', 'bulgarian', 'burgers', 'raw_food', 'icecream', 'brewpubs', 'mideastern', 'syrian', 'cheesesteaks', 'belgian', 'food', 'peruvian', 'filipino', 'korean', 'soulfood', 'pizza', 'russian', 'bangladeshi', 'caribbean', 'cocktailbars', 'churros', 'tradamerican', 'indonesian', 'wine_bars', 'coffeeroasteries', 'greek', 'french', 'cambodian', 'conveyorsushi', 'themedcafes', 'thai', 'piadina', 'honey', 'fondue', 'sportsbars', 'asianfusion', 'cakeshop', 'slovakian', 'ramen', 'british', 'southafrican', 'empanadas', 'chimneycakes', 'latin', 'tapas', 'diners', 'chicken_wings', 'northernmexican', 'macarons', 'italian', 'beergardens', 'hungarian', 'hotpot', 'cideries', 'shavedsnow', 'vegetarian', 'egyptian', 'ethiopian', 'trinidadian', 'taiwanese', 'portuguese', 'chocolate', 'cafes', 'lebanese', 'soup', 'hkcafe', 'burmese', 'bubbletea', 'bistros', 'malaysian', 'seafood', 'austrian', 'japanese', 'cantonese', 'halal', 'nicaraguan', 'srilankan', 'juicebars', 'diyfood', 'scottish', 'honduran', 'scandinavian', 'gastropubs', 'venezuelan', 'armenian', 'speakeasies', 'coffee', 'spanish', 'tikibars', 'bakeries', 'tea', 'smokehouse', 'turkish', 'izakaya', 'champagne_bars', 'waffles', 'gaybars', 'cajun', 'haitian', 'szechuan', 'fishnchips', 'afghani', 'african', 'czech', 'kombucha', 'shanghainese', 'breakfast_brunch', 'mongolian', 'pakistani', 'gourmet', 'tuscan', 'uzbek', 'persian', 'streetvendors'}
    # Just bars
    BARS_CATEGORIES = {'bars', 'divebars', 'winetastingroom', 'meaderies', 'whiskeybars', 'vermouthbars', 'wineries', 'pubs', 'irish_pubs', 'beerbar', 'tapas', 'breweries', 'brewpubs', 'cocktailbars', 'winebars', 'sportsbars', 'beergardens', 'gastropubs', 'tikibars', 'champagne_bars', 'gaybars'}
    # Just food no alcohol
    FOOD_CATEGORIES = {'waffles', 'haitian', 'filipino', 'diners', 'afghani', 'belgian', 'teppanyaki', 'lebanese', 'piadina', 'hotpot', 'cuban', 'bistros', 'singaporean', 'comfortfood', 'buffets', 'soup', 'vegetarian', 'newcanadian', 'tradamerican', 'himalayan', 'dominican', 'themedcafes', 'austrian', 'japanese', 'modern_european', 'ramen', 'hotdog', 'cakeshop', 'portuguese', 'newmexican', 'taiwanese', 'vegan', 'uzbek', 'fishnchips', 'british', 'kosher', 'guamanian', 'szechuan', 'mexican', 'macarons', 'acaibowls', 'georgian', 'bagels', 'cheesesteaks', 'food', 'bakeries', 'persian', 'arabian', 'mediterranean', 'colombian', 'burgers', 'food_court', 'sandwiches', 'shavedsnow', 'coffee', 'trinidadian', 'russian', 'italian', 'diyfood', 'sicilian', 'ukrainian', 'churros', 'tuscan', 'cantonese', 'spanish', 'foodtrucks', 'moroccan', 'sardinian', 'cideries', 'hawaiian', 'soulfood', 'thai', 'falafel', 'gourmet', 'chimneycakes', 'scandinavian', 'izakaya', 'honey', 'cupcakes', 'japacurry', 'bulgarian', 'streetvendors', 'pretzels', 'pancakes', 'salvadoran', 'srilankan', 'delis', 'tex-mex', 'cajun', 'polish', 'dimsum', 'bangladeshi', 'eritrean', 'halal', 'african', 'turkish', 'somali', 'hotdogs', 'slovakian', 'chicken_wings', 'southern', 'brazilian', 'hainan', 'venezuelan', 'winebars', 'dinnertheater', 'empanadas', 'hungarian', 'pakistani', 'poutineries', 'irish', 'cafes', 'asianfusion', 'speakeasies', 'chocolate', 'basque', 'chinese', 'syrian', 'calabrian', 'armenian', 'puertorican', 'southafrican', 'polynesian', 'senegalese', 'donuts', 'german', 'scottish', 'kombucha', 'jaliscan', 'seafood', 'indpak', 'czech', 'juicebars', 'mongolian', 'tapasmallplates', 'australian', 'noodles', 'oaxacan', 'shanghainese', 'wraps', 'bbq', 'burmese', 'foodstands', 'kebab', 'honduran', 'salad', 'gelato', 'korean', 'panasian', 'rotisserie_chicken', 'pizza', 'greek', 'ethiopian', 'laotian', 'malaysian', 'smokehouse', 'nicaraguan', 'vietnamese', 'caribbean', 'hkcafe', 'steak', 'raw_food', 'restaurants', 'peruvian', 'bubbletea', 'poke', 'pastashops', 'tea', 'yucatan', 'sushi', 'wine_bars', 'conveyorsushi', 'french', 'tacos', 'argentine', 'northernmexican', 'fondue', 'chickenshop', 'gluten_free', 'icecream', 'breakfast_brunch', 'tamales', 'cambodian', 'mideastern', 'indonesian', 'latin', 'coffeeroasteries', 'newamerican', 'desserts', 'creperies', 'egyptian', 'iberian'}
    # Ethnicities for subset analysis
    ETHNICITIES_CATEGORIES = {'japanese', 'greek', 'mideastern', 'uzbek', 'southern', 'arabian', 'eritrean', 'irish', 'oaxacan', 'cantonese', 'colombian', 'szechuan', 'puertorican', 'halal', 'laotian', 'armenian', 'basque', 'austrian', 'korean', 'bangladeshi', 'poutineries', 'somali', 'italian', 'bulgarian', 'yucatan', 'russian', 'dominican', 'latin', 'sardinian', 'filipino', 'lebanese', 'asianfusion', 'newmexican', 'senegalese', 'ukrainian', 'sicilian', 'australian', 'vietnamese', 'polynesian', 'georgian', 'southafrican', 'hkcafe', 'pakistani', 'mexican', 'peruvian', 'tradamerican', 'mongolian', 'portuguese', 'burmese', 'moroccan', 'cajun', 'hainan', 'brazilian', 'caribbean', 'honduran', 'himalayan', 'venezuelan', 'kosher', 'scottish', 'northernmexican', 'calabrian', 'tuscan', 'singaporean', 'cuban', 'chinese', 'hungarian', 'srilankan', 'panasian', 'syrian', 'afghani', 'argentine', 'spanish', 'modern_european', 'tex-mex', 'slovakian', 'cambodian', 'thai', 'belgian', 'czech', 'jaliscan', 'taiwanese', 'newamerican', 'scandinavian', 'malaysian', 'african', 'french', 'guamanian', 'shanghainese', 'british', 'indonesian', 'trinidadian', 'iberian', 'salvadoran', 'indpak', 'ethiopian', 'persian', 'polish', 'nicaraguan', 'newcanadian', 'hawaiian', 'haitian', 'german', 'egyptian', 'turkish'}

    df = pd.read_json(path)
    
    transformed_dataframe = df.copy()
    if 'price' not in transformed_dataframe.columns:
        transformed_dataframe['price'] = None
    # Section 2. Clean categories
    # convert 'categories' that were imported as strings to list of dictionaries
    transformed_dataframe['categories'] = transformed_dataframe['categories'].apply(lambda x: json.loads(json.dumps(x)))
    # then transform them into lists that only have alias
    transformed_dataframe['categories'] = transformed_dataframe['categories'].apply(
        lambda x: [category['alias'] for category in x])
    # filter for restaurant categories that are included in food and bars categories
    transformed_dataframe = transformed_dataframe[transformed_dataframe['categories'].apply(
        lambda x: any(cat in FOOD_AND_BARS_CATEGORIES for cat in x))]
    # filter out categories that are definitely useless
    transformed_dataframe = transformed_dataframe[transformed_dataframe['categories'].apply(
        lambda x: not any(cat in DEFINITELY_NO_CATEGORIES for cat in x))]
    transformed_dataframe['ethnic_category'] = transformed_dataframe['categories'].apply(
        lambda x: [category for category in x if category in ETHNICITIES_CATEGORIES]
                    if any(category in ETHNICITIES_CATEGORIES for category in x)
                    else ['Not Specified'])

    # Section 3. Clean coordinates and locations
    # convert 'coordinates' that were imported as strings to python dictionaries
    transformed_dataframe['coordinates'] = transformed_dataframe['coordinates'].apply(lambda x: json.loads(json.dumps(x)))
    transformed_dataframe['latitude'] = transformed_dataframe['coordinates'].apply(lambda x: x['latitude'])
    transformed_dataframe['longitude'] = transformed_dataframe['coordinates'].apply(lambda x: x['longitude'])
    
    # convert 'location' that were imported as strings to dictionaries
    transformed_dataframe['location'] = transformed_dataframe['location'].apply(lambda x: json.loads(json.dumps(x)))
    # only have addresses that are in "CA" for state and start with 9 for 'zip_code'
    transformed_dataframe = transformed_dataframe[transformed_dataframe['location'].apply(
        lambda x: x['state'] == 'CA' and x['city'] in set_cities)]
    # add 'city' column for easy parsing in the future
    transformed_dataframe['city'] = transformed_dataframe['location'].apply(
        lambda x: (str(x['city']).replace('  ', ' ').replace(',', '').lower().rstrip()))
    # clean up 'location' column from dictionary to usual address
    transformed_dataframe['address'] = transformed_dataframe['location'].apply(
        lambda x: (str(x['address1']) + ', ' + str(x['city'].rstrip()) + ' ' + str(x['state']) + ' ' + str(x['zip_code'])) if (
                    x['address2'] is None or x['address2'] == '') else (
                    str(x['address1']) + ' ' + str(x['address2']) + ', '
                    + str(x['city'].replace('  ', ' ').replace(',', '').rstrip())
                    + ' ' + str(x['state']) + ' ' + str(x['zip_code'])))
    # find missing coordinates
    c = transformed_dataframe.loc[transformed_dataframe['latitude'].isna()]\
        .apply(lambda x: geolocate_with_address(x), axis = 1)
    transformed_dataframe.loc[transformed_dataframe['latitude'].isna()] = c


    # delete any 'address' that start with ',' (ex. ', San Jose CA')
    transformed_dataframe['address'] = transformed_dataframe['address'].str.lstrip(', ')

    
    transformed_dataframe = transformed_dataframe[['id', 'alias', 'name', 'url', 'review_count',
        'categories', 'ethnic_category', 'rating', 'price', 'latitude', 'longitude', 'city', 'address']] # .reset_index(drop=True)
     
    # transformed_dataframe = transformed_dataframe.astype(str)
    row_count = len(transformed_dataframe)
    export_data = transformed_dataframe.to_json(orient='records', lines=True)
    
    return export_data, row_count

@task()
def write_bq(df:json, term:str) -> "bigquery.Table":
    """
    Write DataFrame to BigQuery
    """
    # Construct a BigQuery client object.

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    table_id =f"yelp_data_raw.{term}_data_raw"
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("alias", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("review_count", "INTEGER"),
            bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
            bigquery.SchemaField("ethnic_category", "STRING", mode="REPEATED"),
            bigquery.SchemaField("rating", "FLOAT"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("address", "STRING")
            ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,

    )


    job = client.load_table_from_file(
        io.StringIO(df), table_id, job_config=job_config)
    
    # path = Path(f"transformed_data/transformed_{term}-{location}-{index}.json")
    # with open(path, 'rb') as f:
    #     job = client.load_table_from_file(f, table_id, job_config=job_config)

    try:
        job.result()
    except Exception as e:
        print(job.errors)
   

@flow(log_prints=True)
def etl_gcs_to_bq(terms: list, start_slice: int, end_slice: int):
    """
    
    """
    total_rows = 0
    df_locations = fetch_location_df("california_lat_long_cities.csv")
    set_cities = set(df_locations['Name'])
    for term in terms: 
        for i in range(start_slice, end_slice):
            print(term, df_locations.iloc[i]['Name'], i)
    
            gcs_path = extract_from_gcs(term, df_locations.iloc[i]['Name'], i)
            df, row_count = read_json_transform_df(gcs_path, set_cities)
            # path_for_file_save = write_local(df, term, df_locations.iloc[i]['Name'], i)
            write_bq(df, term)
            
    
            # pd.set_option('display.max_columns', 500)
            # pd.set_option('display.width', 1000)

            total_rows += row_count
            print(f"total rows: {total_rows} for {term}")

if __name__ == "__main__":
    TERMS = ['Food'] # ['Juice Bars & Smoothies', 'Desserts', 'Bakeries', 'Coffee & Tea', 'Bubble Tea'] ['Restaurants', 'Food']
    START_SLICE = 0 #For all, START_SLICE = 0, END_SLICE = 459; For LA, START_SLICE = 229 END_SLICE = 230
    END_SLICE = 459 
    
    etl_gcs_to_bq(TERMS, START_SLICE, END_SLICE)

    # prefect deployment build prefect/yelp_gcs_to_bq.py:etl_gcs_to_bq -n "Yelp ELT GCS to BQ"
    # prefect deployment apply etl_gcs_to_bq-deployment.yaml 
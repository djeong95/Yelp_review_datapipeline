"""
Data extraction from Yelp Fusion API.

First part of the script is meant to use the Yelp Fusion API by using the Search API to query for businesses
by a list of search terms and locations.

Please refer to https://docs.developer.yelp.com/docs/get-started for the API
documentation.

Second part of the script is meant to store the queried data into a db format
"""
import os
import mysql.connector
import requests
import time
import json
import pandas as pd
from geopy.geocoders import Nominatim
import numpy as np
import matplotlib.pyplot as plt


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
    df_locations = pd.read_csv("cal_cities_lat_long_MtoZ.csv")
except FileNotFoundError:
    print("CSV File not found.")

# Assign url and api_key for Yelp Fusion API
URL = 'https://api.yelp.com/v3/businesses/search'

API_KEY = os.getenv("YELP_API_KEY")  # your api key

HEADERS = {
    'Authorization': 'Bearer %s' % API_KEY
}

TERMS = ['Bakeries'] # ['Juice Bars & Smoothies', 'Desserts', 'Bakeries', 'Coffee & Tea', 'Bubble Tea']

# ['Restaurants', 'Food']
#

# Specify parameters for API
# Torrance,33.83585,-118.340628 / iloc[415:416]

db_HOST = os.getenv("DB_HOST")
db_USER = os.getenv("DB_USER")
db_PASSWORD = os.getenv("DB_PWD")
db_DATABASE = 'yelpdb'
db_TABLE_NAME = "yelp_cafe" # yelp_cafe_desserts_boba_etc

# Food AtoL and MtoZ on yelp_data; delete yelp_restaurants; copy distinct over to yelp_restaurants


NONFOOD_CATEGORIES = {'copyshops', 'bikes', 'seafoodmarkets', 'cheese','importedfood','buildingsupplies', 'pumpkinpatches', 'hardware', 'vitaminssupplements', 'cannabisdispensaries', 'artclasses', 'horse_boarding', 'pilates', 'sewingalterations', 'reiki', 'musicians', 'educationservices', 'servicestations', 'skishops',  'marketing', 'fleamarkets', 'spiritual_shop', 'tiling', 'carwash', 'selfstorage',  'hiking', 'accessories', 'alternativemedicine', 'localservices', 'convenience', 'indoor_playcenter', 'saunas', 'grocery', 'shipping_centers', 'playgrounds', 'markets', 'weddingchappels', 'bouncehouserentals', 'tabletopgames', 'doulas', 'photographystores', 'tcm', 'icedelivery', 'pettingzoos', 'wholesalers', 'pianobars', 'surfshop', 'couriers', 'homecleaning', 'foodbanks', 'acupuncture', 'personal_shopping', 'computers', 'oilchange', 'boattours', 'escapegames', 'shavedice', 'brasseries', 'custommerchandise', 'sportswear', 'drivethrubars', 'buddhist_temples', 'weightlosscenters', 'balloonservices', 'registrationservices', 'catering', 'usedbooks', 'karaoke', 'thrift_stores', 'vintage', 'autocustomization', 'souvenirs', 'brewingsupplies', 'parking', 'vocation', 'beautysvc', 'tastingclasses', 'hottubandpool', 'flowers', 'distilleries', 'csa', 'ranches', 'eventservices', 'paintball', 'interiordesign', 'wigs', 'magicians', 'appliances', 'limos', 'plumbing', 'irrigation', 'spraytanning', 'organic_stores', 'candlestores', 'airport_shuttles', 'fishing', 'dog_parks', 'horseracing', 'bingo', 'cookingclasses', 'bike_repair_maintenance', 'animalshelters', 'homedecor', 'gardens', 'tours', 'womenscloth', 'laboratorytesting', 'firewood', 'paydayloans', 'eventplanning', 'headshops', 'hobbyshops', 'publicmarkets', 'bocceball', 'gyms', 'grillservices', 'cardioclasses', 'countryclubs', 'reststops', 'nonprofit', 'shoppingcenters', 'gamemeat', 'religiousitems', 'electronics', 'eatertainment', 'aerialfitness', 'pest_control', 'florists', 'stadiumsarenas', 'kitchenincubators', 'costumes', 'waterpurification', 'hookah_bars', 'djs', 'football', 'shopping', 'physicaltherapy', 'cafeteria', 'barcrawl', 'amateursportsteams', 'suppliesrestaurant', 'bikerentals', 'campgrounds', 'tobaccoshops', 'boating', 'beaches', 'farms', 'vapeshops', 'countrydancehalls', 'gardening', 'cyclingclasses', 'partyequipmentrentals', 'pet_sitting', 'gemstonesandminerals', 'religiousorgs', 'cigarbars', 'elementaryschools', 'supperclubs', 'watersuppliers', 'culturalcenter', 'hotels', 'travelservices', 'mini_golf', 'evchargingstations', 'childcloth', 'artschools', 'outdoorgear', 'vinyl_records', 'reflexology', 'waterdelivery', 'international', 'butcher', 'fitness', 'laundryservices', 'videoandgames', 'paintandsip', 'visitorcenters', 'attractionfarms', 'bootcamps', 'farmersmarket', 'beachequipmentrental', 'trains', 'landmarks', 'golf', 'resorts', 'petadoption', 'winetasteclasses', 'partycharacters', 'mobilephones', 'outlet_stores', 'boatcharters', 'groomer', 'itservices', 'petboarding', 'homehealthcare', 'customcakes', 'chiropractors', 'sommelierservices', 'healthcoach', 'nutritionists', 'artmuseums', 'popupshops', 'museums', 'service_stations', 'fireworks', 'wholesale_stores', 'battingcages', 'fueldocks', 'antiques', 'homeandgarden', 'artsandcrafts', 'galleries', 'livestocksupply', 'surfing', 'personalchefs', 'vacation_rentals', 'amusementparks', 'hats', 'bodyshops', 'cosmetics', 'casinos', 'hospitals', 'eyebrowservices', 'postoffices', 'musicvenues', 'giftshops', 'tennis', 'guesthouses', 'opticians', 'swimmingpools', 'yoga', 'stationery', 'christmastrees', 'beer_and_wine', 'cookingschools', 'bedbreakfast', 'rvparks', 'rugs', 'autorepair', 'autopartssupplies', 'discountstore', 'tradclothing', 'discgolf', 'fashion', 'foodtours', 'recreation', 'pharmacy', 'petstore', 'meats', 'paddleboarding', 'hotelstravel', 'kiteboarding', 'psychic_astrology', 'parks', 'internetcafe', 'specialtyschools', 'kitchensupplies', 'lingerie', 'bookstores', 'arcades', 'lancenters', 'toys', 'threadingservices', 'lifecoach', 'herbsandspices', 'publicservicesgovt', 'auto', 'marinas', 'taxis', 'huntingfishingsupplies', 'health', 'medcenters', 'kitchenandbath', 'kids_activities', 'dancestudio', 'massage_therapy', 'pet_training', 'oliveoil', 'axethrowing', 'bowling', 'barbers', 'kiosk', 'intlgrocery', 'localflavor', 'bartenders', 'petbreeders', 'laundromat', 'musicvideo', 'arts', 'yelpevents', 'candy', 'watches', 'churches', 'waterstores', 'winetours', 'herbalshops', 'popcorn', 'deptstores', 'medicalspa', 'pickyourown', 'jewelry', 'fabricstores', 'truckrepair', 'movietheaters', 'guns_and_ammo', 'education', 'advertising', 'sportgoods', 'partysupplies', 'floraldesigners', 'skincare', 'popuprestaurants', 'horsebackriding', 'healthtrainers', 'poolhalls', 'mags', 'homeappliancerepair', 'golflessons', 'beverage_stores', 'smog_check_stations', 'businessconsulting', 'beertours', 'propane', 'nikkei', 'walkingtours', 'cosmeticdentists', 'artspacerentals', 'sharedofficespaces', 'fooddeliveryservices', 'menscloth', 'clothingrental', 'venues', 'shoes', 'nightlife', 'massage', 'drugstores', 'cabaret', 'lounges', 'active', 'healthmarkets', 'specialed', 'photoboothrentals', 'spas', 'wedding_planning', 'teambuilding', 'festivals', 'naturopathic', 'comedyclubs', 'social_clubs', 'danceclubs', 'jazzandblues', 'wildlifecontrol', 'outdoormovies', 'furniture', 'mobilephonerepair', 'internalmed', 'meditationcenters', 'theater', 'virtualrealitycenters'}
DEFINITELY_NO_CATEGORIES = {'grocery', 'convenience', 'drugstores'}
ALL_CATEGORIES = {'delis', 'copyshops', 'japanese', 'bikes', 'buildingsupplies', 'pumpkinpatches', 'hardware', 'greek', 'honey', 'winetastingroom', 'vitaminssupplements', 'cannabisdispensaries', 'acaibowls', 'artclasses', 'horse_boarding', 'diners', 'foodtrucks', 'pilates', 'sewingalterations', 'reiki', 'musicians', 'educationservices', 'servicestations', 'skishops', 'bistros', 'mideastern', 'raw_food', 'marketing', 'fleamarkets', 'cheesesteaks', 'hotpot', 'uzbek', 'spiritual_shop', 'tiling', 'breweries', 'diyfood', 'carwash', 'selfstorage', 'southern', 'arabian', 'coffee', 'eritrean', 'hiking', 'fishnchips', 'accessories', 'alternativemedicine', 'kombucha', 'localservices', 'convenience', 'indoor_playcenter', 'sushi', 'pubs', 'irish', 'oaxacan', 'cantonese', 'japacurry', 'saunas', 'grocery', 'colombian', 'szechuan', 'shipping_centers', 'izakaya', 'playgrounds', 'markets', 'weddingchappels', 'bouncehouserentals', 'tabletopgames', 'cocktailbars', 'doulas', 'photographystores', 'chocolate', 'tcm', 'dinnertheater', 'icedelivery', 'food', 'pettingzoos', 'puertorican', 'halal', 'wholesalers', 'pianobars', 'surfshop', 'laotian', 'couriers', 'homecleaning', 'foodbanks', 'armenian', 'divebars', 'acupuncture', 'basque', 'austrian', 'personal_shopping', 'computers', 'korean', 'oilchange', 'boattours', 'escapegames', 'shavedice', 'brasseries', 'custommerchandise', 'falafel', 'sportswear', 'drivethrubars', 'buddhist_temples', 'bangladeshi', 'poutineries', 'weightlosscenters', 'balloonservices', 'somali', 'gourmet', 'registrationservices', 'catering', 'streetvendors', 'usedbooks', 'karaoke', 'thrift_stores', 'themedcafes', 'vintage', 'autocustomization', 'souvenirs', 'brewingsupplies', 'italian', 'bulgarian', 'parking', 'yucatan', 'russian', 'cakeshop', 'vocation', 'beautysvc', 'tastingclasses', 'hottubandpool', 'flowers', 'distilleries', 'csa', 'ranches', 'teppanyaki', 'cupcakes', 'eventservices', 'paintball', 'interiordesign', 'dominican', 'latin', 'wigs', 'magicians', 'sardinian', 'appliances', 'limos', 'plumbing', 'irrigation', 'churros', 'spraytanning', 'filipino', 'lebanese', 'organic_stores', 'candlestores', 'airport_shuttles', 'coffeeroasteries', 'fishing', 'asianfusion', 'newmexican', 'dog_parks', 'horseracing', 'bingo', 'cookingclasses', 'bike_repair_maintenance', 'animalshelters', 'champagne_bars', 'homedecor', 'senegalese', 'ukrainian', 'gardens', 'tours', 'womenscloth', 'laboratorytesting', 'firewood', 'paydayloans', 'eventplanning', 'headshops', 'noodles', 'hobbyshops', 'publicmarkets', 'wine_bars', 'sicilian', 'tea', 'bocceball', 'gyms', 'juicebars', 'grillservices', 'cardioclasses', 'comfortfood', 'soup', 'wraps', 'countryclubs', 'reststops', 'nonprofit', 'australian', 'restaurants', 'vermouthbars', 'shoppingcenters', 'gastropubs', 'gamemeat', 'bakeries', 'bagels', 'religiousitems', 'desserts', 'electronics', 'eatertainment', 'aerialfitness', 'pest_control', 'florists', 'chicken_wings', 'stadiumsarenas', 'seafood', 'kitchenincubators', 'costumes', 'waterpurification', 'hookah_bars', 'djs', 'football', 'shopping', 'physicaltherapy', 'cafeteria', 'rotisserie_chicken', 'cideries', 'barcrawl', 'amateursportsteams', 'suppliesrestaurant', 'bikerentals', 'campgrounds', 'vietnamese', 'tobaccoshops', 'boating', 'gaybars', 'polynesian', 'beaches', 'pancakes', 'farms', 'vapeshops', 'countrydancehalls', 'gardening', 'cyclingclasses', 'partyequipmentrentals', 'pet_sitting', 'gemstonesandminerals', 'religiousorgs', 'cigarbars', 'elementaryschools', 'supperclubs', 'watersuppliers', 'georgian', 'macarons', 'culturalcenter', 'southafrican', 'hotels', 'travelservices', 'mini_golf', 'evchargingstations', 'hkcafe', 'childcloth', 'artschools', 'outdoorgear', 'vinyl_records', 'reflexology', 'waterdelivery', 'international', 'butcher', 'pakistani', 'fitness', 'piadina', 'mexican', 'tamales', 'brewpubs', 'laundryservices', 'poke', 'videoandgames', 'empanadas', 'paintandsip', 'visitorcenters', 'attractionfarms', 'buffets', 'chickenshop', 'peruvian', 'tradamerican', 'bootcamps', 'mongolian', 'farmersmarket', 'beachequipmentrental', 'trains', 'landmarks', 'golf', 'resorts', 'portuguese', 'tikibars', 'petadoption', 'winetasteclasses', 'partycharacters', 'shavedsnow', 'pastashops', 'gelato', 'burmese', 'mobilephones', 'outlet_stores', 'boatcharters', 'gluten_free', 'chimneycakes', 'moroccan', 'cajun', 'breakfast_brunch', 'speakeasies', 'hainan', 'groomer', 'itservices', 'petboarding', 'homehealthcare', 'customcakes', 'brazilian', 'caribbean', 'chiropractors', 'sommelierservices', 'healthcoach', 'nutritionists', 'artmuseums', 'popupshops', 'museums', 'honduran', 'service_stations', 'fireworks', 'himalayan', 'wholesale_stores', 'venezuelan', 'battingcages', 'fueldocks', 'kosher', 'antiques', 'foodstands', 'bubbletea', 'scottish', 'homeandgarden', 'artsandcrafts', 'galleries', 'livestocksupply', 'surfing', 'pretzels', 'personalchefs', 'hotdogs', 'donuts', 'vacation_rentals', 'amusementparks', 'hats', 'bodyshops', 'cosmetics', 'casinos', 'northernmexican', 'hospitals', 'eyebrowservices', 'vegetarian', 'postoffices', 'musicvenues', 'giftshops', 'cheese', 'calabrian', 'tennis', 'guesthouses', 'opticians', 'swimmingpools', 'yoga', 'stationery', 'tuscan', 'singaporean', 'cuban', 'christmastrees', 'beer_and_wine', 'cookingschools', 'bedbreakfast', 'chinese', 'rvparks', 'rugs', 'autorepair', 'autopartssupplies', 'discountstore', 'tradclothing', 'discgolf', 'fashion', 'foodtours', 'recreation', 'pharmacy', 'petstore', 'hungarian', 'srilankan', 'panasian', 'food_court', 'meats', 'paddleboarding', 'syrian', 'hotelstravel', 'kiteboarding', 'sandwiches', 'seafoodmarkets', 'soulfood', 'psychic_astrology', 'conveyorsushi', 'parks', 'internetcafe', 'specialtyschools', 'kitchensupplies', 'lingerie', 'bookstores', 'arcades', 'lancenters', 'toys', 'kebab', 'threadingservices', 'lifecoach', 'herbsandspices', 'publicservicesgovt', 'auto', 'cafes', 'afghani', 'marinas', 'taxis', 'huntingfishingsupplies', 'fondue', 'argentine', 'health', 'sportsbars', 'spanish', 'modern_european', 'medcenters', 'kitchenandbath', 'kids_activities', 'dancestudio', 'massage_therapy', 'tex-mex', 'pe'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 't_training', 'slovakian', 'importedfood', 'oliveoil', 'burgers', 'axethrowing', 'bowling', 'barbers', 'kiosk', 'intlgrocery', 'cambodian', 'localflavor', 'bartenders', 'petbreeders', 'laundromat', 'musicvideo', 'arts', 'yelpevents', 'candy', 'thai', 'watches', 'churches', 'belgian', 'waterstores', 'winetours', 'herbalshops', 'czech', 'popcorn', 'deptstores', 'jaliscan', 'medicalspa', 'taiwanese', 'meaderies', 'pickyourown', 'jewelry', 'icecream', 'fabricstores', 'creperies', 'truckrepair', 'newamerican', 'scandinavian', 'salad', 'movietheaters', 'malaysian', 'guns_and_ammo', 'education', 'advertising', 'sportgoods', 'partysupplies', 'floraldesigners', 'skincare', 'african', 'popuprestaurants', 'horsebackriding', 'mediterranean', 'vegan', 'healthtrainers', 'poolhalls', 'french', 'guamanian', 'beerbar', 'mags', 'beergardens', 'homeappliancerepair', 'golflessons', 'beverage_stores', 'smog_check_stations', 'shanghainese', 'businessconsulting', 'beertours', 'propane', 'smokehouse', 'tapas', 'nikkei', 'british', 'walkingtours', 'indonesian', 'cosmeticdentists', 'artspacerentals', 'wineries', 'sharedofficespaces', 'irish_pubs', 'fooddeliveryservices', 'menscloth', 'clothingrental', 'venues', 'shoes', 'nightlife', 'massage', 'bars', 'drugstores', 'trinidadian', 'cabaret', 'lounges', 'iberian', 'ramen', 'salvadoran', 'steak', 'dimsum', 'indpak', 'pizza', 'waffles', 'active', 'healthmarkets', 'ethiopian', 'persian', 'specialed', 'photoboothrentals', 'spas', 'wedding_planning', 'teambuilding', 'tapasmallplates', 'polish', 'hotdog', 'nicaraguan', 'festivals', 'newcanadian', 'hawaiian', 'naturopathic', 'comedyclubs', 'haitian', 'social_clubs', 'danceclubs', 'whiskeybars', 'german', 'jazzandblues', 'wildlifecontrol', 'outdoormovies', 'furniture', 'mobilephonerepair', 'internalmed', 'bbq', 'meditationcenters', 'tacos', 'theater', 'egyptian', 'virtualrealitycenters', 'turkish'}
FOOD_AND_BARS_CATEGORIES = {'poutineries', 'sardinian', 'bars', 'divebars', 'tamales', 'winetastingroom', 'comfortfood', 'somali', 'rotisserie_chicken', 'cuban', 'puertorican', 'pastashops', 'meaderies', 'whiskeybars', 'indpak', 'vermouthbars', 'moroccan', 'cupcakes', 'newmexican', 'polynesian', 'falafel', 'pancakes', 'yucatan', 'bbq', 'oaxacan', 'ukrainian', 'arabian', 'foodstands', 'brazilian', 'wineries', 'japacurry', 'pubs', 'himalayan', 'newcanadian', 'tacos', 'australian', 'hainan', 'dinnertheater', 'foodtrucks', 'acaibowls', 'georgian', 'sicilian', 'calabrian', 'irish_pubs', 'dimsum', 'kebab', 'teppanyaki', 'colombian', 'newamerican', 'chinese', 'vietnamese', 'delis', 'irish', 'eritrean', 'southern', 'wraps', 'senegalese', 'tapasmallplates', 'hawaiian', 'guamanian', 'mediterranean', 'hotdog', 'vegan', 'laotian', 'panasian', 'polish', 'beerbar', 'argentine', 'steak', 'sandwiches', 'tex-mex', 'bagels', 'german', 'salad', 'creperies', 'gluten_free', 'pretzels', 'poke', 'jaliscan', 'noodles', 'buffets', 'basque', 'iberian', 'chickenshop', 'singaporean', 'modern_european', 'desserts', 'donuts', 'food_court', 'breweries', 'mexican', 'sushi', 'dominican', 'salvadoran', 'restaurants', 'hotdogs', 'gelato', 'kosher', 'bulgarian', 'burgers', 'raw_food', 'icecream', 'brewpubs', 'mideastern', 'syrian', 'cheesesteaks', 'belgian', 'food', 'peruvian', 'filipino', 'korean', 'soulfood', 'pizza', 'russian', 'bangladeshi', 'caribbean', 'cocktailbars', 'churros', 'tradamerican', 'indonesian', 'wine_bars', 'coffeeroasteries', 'greek', 'french', 'cambodian', 'conveyorsushi', 'themedcafes', 'thai', 'piadina', 'honey', 'fondue', 'sportsbars', 'asianfusion', 'cakeshop', 'slovakian', 'ramen', 'british', 'southafrican', 'empanadas', 'chimneycakes', 'latin', 'tapas', 'diners', 'chicken_wings', 'northernmexican', 'macarons', 'italian', 'beergardens', 'hungarian', 'hotpot', 'cideries', 'shavedsnow', 'vegetarian', 'egyptian', 'ethiopian', 'trinidadian', 'taiwanese', 'portuguese', 'chocolate', 'cafes', 'lebanese', 'soup', 'hkcafe', 'burmese', 'bubbletea', 'bistros', 'malaysian', 'seafood', 'austrian', 'japanese', 'cantonese', 'halal', 'nicaraguan', 'srilankan', 'juicebars', 'diyfood', 'scottish', 'honduran', 'scandinavian', 'gastropubs', 'venezuelan', 'armenian', 'speakeasies', 'coffee', 'spanish', 'tikibars', 'bakeries', 'tea', 'smokehouse', 'turkish', 'izakaya', 'champagne_bars', 'waffles', 'gaybars', 'cajun', 'haitian', 'szechuan', 'fishnchips', 'afghani', 'african', 'czech', 'kombucha', 'shanghainese', 'breakfast_brunch', 'mongolian', 'pakistani', 'gourmet', 'tuscan', 'uzbek', 'persian', 'streetvendors'}
BARS_CATEGORIES = {'bars', 'divebars', 'winetastingroom', 'meaderies', 'whiskeybars', 'vermouthbars', 'wineries', 'pubs', 'irish_pubs', 'beerbar', 'tapas', 'breweries', 'brewpubs', 'cocktailbars', 'winebars', 'sportsbars', 'beergardens', 'gastropubs', 'tikibars', 'champagne_bars', 'gaybars'}
FOOD_CATEGORIES = {'waffles', 'haitian', 'filipino', 'diners', 'afghani', 'belgian', 'teppanyaki', 'lebanese', 'piadina', 'hotpot', 'cuban', 'bistros', 'singaporean', 'comfortfood', 'buffets', 'soup', 'vegetarian', 'newcanadian', 'tradamerican', 'himalayan', 'dominican', 'themedcafes', 'austrian', 'japanese', 'modern_european', 'ramen', 'hotdog', 'cakeshop', 'portuguese', 'newmexican', 'taiwanese', 'vegan', 'uzbek', 'fishnchips', 'british', 'kosher', 'guamanian', 'szechuan', 'mexican', 'macarons', 'acaibowls', 'georgian', 'bagels', 'cheesesteaks', 'food', 'bakeries', 'persian', 'arabian', 'mediterranean', 'colombian', 'burgers', 'food_court', 'sandwiches', 'shavedsnow', 'coffee', 'trinidadian', 'russian', 'italian', 'diyfood', 'sicilian', 'ukrainian', 'churros', 'tuscan', 'cantonese', 'spanish', 'foodtrucks', 'moroccan', 'sardinian', 'cideries', 'hawaiian', 'soulfood', 'thai', 'falafel', 'gourmet', 'chimneycakes', 'scandinavian', 'izakaya', 'honey', 'cupcakes', 'japacurry', 'bulgarian', 'streetvendors', 'pretzels', 'pancakes', 'salvadoran', 'srilankan', 'delis', 'tex-mex', 'cajun', 'polish', 'dimsum', 'bangladeshi', 'eritrean', 'halal', 'african', 'turkish', 'somali', 'hotdogs', 'slovakian', 'chicken_wings', 'southern', 'brazilian', 'hainan', 'venezuelan', 'winebars', 'dinnertheater', 'empanadas', 'hungarian', 'pakistani', 'poutineries', 'irish', 'cafes', 'asianfusion', 'speakeasies', 'chocolate', 'basque', 'chinese', 'syrian', 'calabrian', 'armenian', 'puertorican', 'southafrican', 'polynesian', 'senegalese', 'donuts', 'german', 'scottish', 'kombucha', 'jaliscan', 'seafood', 'indpak', 'czech', 'juicebars', 'mongolian', 'tapasmallplates', 'australian', 'noodles', 'oaxacan', 'shanghainese', 'wraps', 'bbq', 'burmese', 'foodstands', 'kebab', 'honduran', 'salad', 'gelato', 'korean', 'panasian', 'rotisserie_chicken', 'pizza', 'greek', 'ethiopian', 'laotian', 'malaysian', 'smokehouse', 'nicaraguan', 'vietnamese', 'caribbean', 'hkcafe', 'steak', 'raw_food', 'restaurants', 'peruvian', 'bubbletea', 'poke', 'pastashops', 'tea', 'yucatan', 'sushi', 'wine_bars', 'conveyorsushi', 'french', 'tacos', 'argentine', 'northernmexican', 'fondue', 'chickenshop', 'gluten_free', 'icecream', 'breakfast_brunch', 'tamales', 'cambodian', 'mideastern', 'indonesian', 'latin', 'coffeeroasteries', 'newamerican', 'desserts', 'creperies', 'egyptian', 'iberian'}
ETHNICITIES_CATEGORIES = {'japanese', 'greek', 'mideastern', 'uzbek', 'southern', 'arabian', 'eritrean', 'irish', 'oaxacan', 'cantonese', 'colombian', 'szechuan', 'puertorican', 'halal', 'laotian', 'armenian', 'basque', 'austrian', 'korean', 'bangladeshi', 'poutineries', 'somali', 'italian', 'bulgarian', 'yucatan', 'russian', 'dominican', 'latin', 'sardinian', 'filipino', 'lebanese', 'asianfusion', 'newmexican', 'senegalese', 'ukrainian', 'sicilian', 'australian', 'vietnamese', 'polynesian', 'georgian', 'southafrican', 'hkcafe', 'pakistani', 'mexican', 'peruvian', 'tradamerican', 'mongolian', 'portuguese', 'burmese', 'moroccan', 'cajun', 'hainan', 'brazilian', 'caribbean', 'honduran', 'himalayan', 'venezuelan', 'kosher', 'scottish', 'northernmexican', 'calabrian', 'tuscan', 'singaporean', 'cuban', 'chinese', 'hungarian', 'srilankan', 'panasian', 'syrian', 'afghani', 'argentine', 'spanish', 'modern_european', 'tex-mex', 'slovakian', 'cambodian', 'thai', 'belgian', 'czech', 'jaliscan', 'taiwanese', 'newamerican', 'scandinavian', 'malaysian', 'african', 'french', 'guamanian', 'shanghainese', 'british', 'indonesian', 'trinidadian', 'iberian', 'salvadoran', 'indpak', 'ethiopian', 'persian', 'polish', 'nicaraguan', 'newcanadian', 'hawaiian', 'haitian', 'german', 'egyptian', 'turkish'}

geolocator = Nominatim(user_agent='my-applications', timeout=10)

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

def insert_data_to_db(data, host, user, password, database, tablename):
    """
    With data pulled from pull_data_across_locations function, access the MySQL Workbench server to
    write into the database
    :param data: A list of dictionary, with each dictionary being a restaurant or a place
    :param host: host for MySQL Workbench
    :param user: user for MySQL Workbench
    :param password: password for my MySQL Workbench
    :param database: database name for MySQL Workbench
    :param tablename: tablename in the database for MySQL Workbench
    """
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
        price, location, phone, display_phone)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tablename) #, distance ,%s

        for item in data:

            #
            select_sql = "SELECT id FROM {} WHERE id ='{}'".format(tablename, item['id'])
            mycursor.execute(select_sql)
            datapiece = mycursor.fetchone()

            if not datapiece:
                # Convert the categories list to a JSON formatted string
                price = item.get('price', None)
                val = (item['id'], item['alias'], item['name'], item['image_url'], item['is_closed'], item['url'],
                       item['review_count'], json.dumps(item['categories']), item['rating'], json.dumps(item['coordinates']),
                       json.dumps(item['transactions']),
                       price, json.dumps(item['location']), item['phone'], item['display_phone'])
                mycursor.execute(sql, val)
        print(data[:10])
        mydb.commit()
        print(mycursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table: {}".format(error))
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed.")

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

            # Get results from get_api_data
            result = get_api_data(url, headers, term, df_locations.iloc[i]['Latitude'],
                                  df_locations.iloc[i]['Longitude'])
            print(term, df_locations.iloc[i]['Name'])
            # Write into MySQL Workbench Server; all db_variables are global except result
            insert_data_to_db(result, db_HOST, db_USER, db_PASSWORD, db_DATABASE, db_TABLE_NAME)
            results.extend(result)

    return results

def fetch_data_from_mysql(host, user, password, database, tablename):
    """

    :param host:
    :param user:
    :param password:
    :param database:
    :param tablename:
    :return:
    """

    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    # Write a SQL query to execute to fetch desired data
    query = "SELECT * FROM {}".format(tablename)

    # Create a cursor object and execute the query
    cursor = mydb.cursor()
    cursor.execute(query)

    # Fetch all rows of the result into a list of tuples
    rows = cursor.fetchall()

    # Get column names
    col_names = [i[0] for i in cursor.description]

    # Create a pandas DataFrame from the rows and column names
    df = pd.DataFrame(rows, columns=col_names)

    # Close cursor and connection to database
    cursor.close()
    mydb.close()

    return df

def geolocate_with_address(row):
    time.sleep(1)
    if pd.isna(row['latitude']) and pd.isna(row['longitude']):
        location = geolocator.geocode(row['address'])
        if location:
            row['latitude'] = location.latitude
            row['longitude'] = location.longitude
    return row

def pandas_transformation(df):
    """

    :param df:
    :return:
    """
    startingtime = time.time()
    transformed_dataframe = df.copy()
    # convert 'categories' that were imported as strings to list of dictionaries
    # then transform them into lists that only have alias
    transformed_dataframe['categories'] = transformed_dataframe['categories'].apply(lambda x: json.loads(x))
    transformed_dataframe['categories'] = transformed_dataframe['categories'].apply(
        lambda x: [category['alias'] for category in x])

    transformed_dataframe = transformed_dataframe[transformed_dataframe['categories'].apply(lambda x: any(cat in FOOD_AND_BARS_CATEGORIES for cat in x))]
    transformed_dataframe = transformed_dataframe[transformed_dataframe['categories'].apply(lambda x: not any(cat in DEFINITELY_NO_CATEGORIES for cat in x))]

    # convert 'categories' that were imported as strings to dictionaries
    transformed_dataframe['coordinates'] = transformed_dataframe['coordinates'].apply(lambda x: json.loads(x))
    transformed_dataframe['latitude'] = transformed_dataframe['coordinates'].apply(lambda x: x['latitude'])
    transformed_dataframe['longitude'] = transformed_dataframe['coordinates'].apply(lambda x: x['longitude'])

    # convert 'location' that were imported as strings to dictionaries
    transformed_dataframe['location'] = transformed_dataframe['location'].apply(lambda x: json.loads(x))

    # only have addresses that are in "CA" for state and start with 9 for 'zip_code'
    transformed_dataframe = transformed_dataframe[transformed_dataframe['location'].apply(lambda x: x['state'] == 'CA' and x['zip_code'].startswith('9'))]

    # add 'city' column for easy parsing in the future
    transformed_dataframe['city'] = transformed_dataframe['location'].apply(lambda x: (str(x['city']).replace('  ', ' ').replace(',', '').lower().rstrip()))

    # clean up 'location' column from dictionary to usual address
    transformed_dataframe['address'] = transformed_dataframe['location'].apply(
        lambda x: (str(x['address1']) + ', ' + str(x['city'].rstrip()) + ' ' + str(x['state']) + ' ' + str(x['zip_code'])) if (
                    x['address2'] is None or x['address2'] == '') else (
                    str(x['address1']) + ' ' + str(x['address2']) + ', '
                    + str(x['city'].replace('  ', ' ').replace(',', '').rstrip())
                    + ' ' + str(x['state']) + ' ' + str(x['zip_code'])))

    # delete any 'address' that start with ', San Jose CA'
    transformed_dataframe['address'] = transformed_dataframe['address'].str.lstrip(', ')
    # a = transformed_dataframe[transformed_dataframe['price'].isna()]
    # print(a['price'])

    c = transformed_dataframe.loc[transformed_dataframe['latitude'].isna()]\
        .apply(lambda x: geolocate_with_address(x), axis = 1)
    transformed_dataframe.loc[transformed_dataframe['latitude'].isna()] = c

    # imputation of price for none (about 33% are missing value for price) - https://krrai77.medium.com/using-fancyimpute-in-python-eadcffece782
    # print(transformed_dataframe['price'].value_counts().sort_values(ascending=False))
    # print(transformed_dataframe['price'].isna().sum(), "this is all null")

    # ids = {'fEoCt8Tb_IfiAeNBt21Dyw', 'ivFOR-t2r-tibmKUwQMEfw', 'O5hM13UxmAlJy9oAazyekA', 't94N88YZuBWRDsw5PLbQiA'}
    # b = transformed_dataframe.loc[transformed_dataframe['id'].isin(ids)] #
    # # print(transformed_dataframe['price'])
    #
    # print(b['latitude'], b['longitude'], b['address'])
    # reduce columns
    transformed_dataframe = transformed_dataframe[['id', 'alias', 'name', 'url', 'review_count',
        'categories', 'rating', 'price', 'latitude', 'longitude', 'city', 'address']] # .reset_index(drop=True)

    export_data = transformed_dataframe.to_dict(orient='records')

    # print("transformed_dataframe took", time.time() - startingtime, "to run")
    return export_data


def insert_transformed_data_to_db(data, host, user, password, database, tablename):
    """
    With data pulled from pull_data_across_locations function, access the MySQL Workbench server to
    write into the database
    :param data: A list of dictionary, with each dictionary being a restaurant or a place
    :param host: host for MySQL Workbench
    :param user: user for MySQL Workbench
    :param password: password for my MySQL Workbench
    :param database: database name for MySQL Workbench
    :param tablename: tablename in the database for MySQL Workbench
    """
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

        sql = "INSERT INTO {} (id, alias, name,\
        url, review_count, categories, rating, price, latitude, longitude, city, address)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tablename)

        for item in data:

            select_sql = "SELECT id FROM {} WHERE id ='{}'".format(tablename, item['id'])
            mycursor.execute(select_sql)
            datapiece = mycursor.fetchone()

            if not datapiece:
                # Convert the categories list to a JSON formatted string
                price = item.get('price', None)
                latitude = item.get('latitude', None)
                longitude = item.get('longitude', None)
                val = (item['id'], item['alias'], item['name'], item['url'], item['review_count'],
                       json.dumps(item['categories']), item['rating'], price, latitude, longitude,
                       item['city'], item['address'])

                # print(f"Executing SQL query: {sql} with values {val}")
                mycursor.execute(sql, val)

        print(data[:10])
        mydb.commit()
        print(mycursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table: {}".format(error))
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed.")


test_result = pull_data_across_locations(URL, HEADERS, TERMS, df_locations[::48]) # after finishing 48, start with [76::]
# DTYPE_DICT = {'id':str, 'alias':str, 'name':str, 'image_url':str, 'is_closed':bool, 'url':str, 'review_count':int, 'categories':list, 'rating':float, 'coordinates':dict, 'transactions':list, 'price':str, 'location':dict, 'phone':str, 'display_phone':str}

# dataframe = fetch_data_from_mysql(db_HOST, db_USER, db_PASSWORD, db_DATABASE, db_TABLE_NAME)
# transformed_df = pandas_transformation(dataframe)
# insert_transformed_data_to_db(transformed_df,db_HOST, db_USER, db_PASSWORD, db_DATABASE, 'yelp_restaurant_cleaned')




print("Code running completed.")
print ("My program took", time.time() - start_time, "to run")

"""
df["categories"] = df["categories"].apply(lambda x: ', '.join(str(element) for element in x))
df['cat_len'] = df['categories'].apply(lambda x: len(x.split(",")))
df['keyterm'] = 'Desserts/Teas/Cafes/Bakeries'
df = df.loc[~df['categories'].str.contains("Grocery")]
df = df.loc[~df['categories'].str.contains("Drug Store")]
df = df.loc[~df['categories'].str.contains("Convenience Store")]
df.loc[df['categories'].str.contains("Breweries"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Food Trucks"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Burgers"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Bars"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Juice Bars"), "keyterm"] = "Desserts/Teas/Cafes/Bakeries" 
df.loc[df['categories'].str.contains("Breakfast & Brunch"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Chicken"), "keyterm"] = "Restaurants" 
df.loc[df['categories'].str.contains("American (Traditional)"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Vietnamese"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Vietnamese") & df['categories'].str.contains("Juice Bars") & df['categories'].str.contains("Bubble Tea"), "keyterm"] = "Desserts/Teas/Cafes/Bakeries"
df.loc[df['categories'].str.contains("Vietnamese") & df['categories'].str.contains("Juice Bars") & df['categories'].str.contains("Desserts"), "keyterm"] = "Desserts/Teas/Cafes/Bakeries"
df.loc[df['categories'].str.contains("Vietnamese") & df['name'].str.contains("Pho"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Meat"), "keyterm"] = "Restaurants"
df.loc[df['categories'].str.contains("Wineries"), "keyterm"] = "Attractions"
df.loc[df['categories'].str.contains("Wine Tasting"), "keyterm"] = "Attractions"

"""
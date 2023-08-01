{{ config(materialized='table') }}

SELECT 
    LOWER(Name) as city,
    Latitude as latitude,
    Longitude as longitude,
    {{ get_coordinates('latitude', 'longitude') }} as coordinate

FROM {{ ref('california_lat_long_cities') }}
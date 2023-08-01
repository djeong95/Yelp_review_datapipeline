{{ config(materialized='table') }}

SELECT
    LOWER(City) as city,
    LOWER(`County Name`) as county,
    State as state,
    Zip as zipcode

FROM {{ ref('california_county_cities') }}



{{ config(materialized='table') }}

with dim_coordinates as (
    select *
    from {{ ref('dim_coordinates') }}
),

dim_counties as (
    select *
    from {{ ref('dim_counties') }}
),

restaurants_data as (
    select *
    from {{ ref('stg_restaurants') }}
),

cafe_data as (
    select *
    from {{ ref('stg_cafes') }}
),

yelp_all as (
    select * from restaurants_data
)

select
    yelp_all.id,
    yelp_all.alias,
    LOWER(yelp_all.name),
    yelp_all.url,
    yelp_all.review_count,
    yelp_all.rating,
    yelp_all.price,
    yelp_all.categories,
    LOWER(yelp_all.ethnic_category),
    LOWER(yelp_all.type),
    yelp_all.latitude as shop_latitude,
    yelp_all.longitude as shop_longitude,
    yelp_all.coordinate,
    yelp_all.address,
    yelp_all.city as shop_city,
    dim_coordinates.city as city_city,
    dim_coordinates.latitude as city_latitude,
    dim_coordinates.longitude as city_longitude,
    dim_coordinates.coordinate as city_coordinate,
    dim_counties.city as county_city,
    dim_counties.county as county,
    dim_counties.zipcode as county_zipcode

from yelp_all
inner join dim_coordinates
on yelp_all.city = dim_coordinates.city
inner join dim_counties
on yelp_all.city = dim_counties.city
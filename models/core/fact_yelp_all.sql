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

desserts_data as (
    select *
    from {{ ref('stg_desserts') }}
    where (categories != "coffee") 
      AND (categories NOT LIKE "%coffeeroasteries%") 
      AND (categories NOT LIKE "%wine%") 
      AND (categories NOT LIKE "%burgers%") 
      AND (categories NOT LIKE "%hotdogs%") 
      AND (categories NOT LIKE "%breakfast_brunch%") 
      AND (categories NOT LIKE "%breweries%") 
      AND (categories NOT LIKE "%brewpubs%") 
      AND (categories NOT LIKE "%pizza%") 
      AND (categories NOT LIKE "%coffee, sandwiches%") 
      AND (categories NOT LIKE "%foodtrucks%") 
      AND (categories NOT LIKE "%sandwiches, coffee%") 
      AND (categories NOT LIKE "%coffee, sandwiches%") 
      AND (categories NOT LIKE "%market%") 
      AND (categories NOT LIKE "%poke%") 
      AND (categories NOT LIKE "%pretzel%") 
      AND (categories NOT LIKE "%chicken%") 
      AND (alias NOT LIKE "pho")
),

cafe_data as (
    select *
    from {{ ref('stg_cafes') }}
    where (categories LIKE '%desserts%' AND categories LIKE '%juicebars%' AND categories LIKE '%coffee%')
        OR (categories LIKE '%cafes%')
        OR (LOWER(name) LIKE '%cafe%' AND categories LIKE '%cafes%')
        OR (LOWER(name) LIKE '%coffee%' AND categories LIKE '%cafes%')
        OR (categories LIKE '%coffee%' AND categories LIKE '%desserts%')
        OR (categories LIKE '%coffee%' AND ARRAY_LENGTH(SPLIT(categories, ',')) = 1)
        OR (categories LIKE '%tea%')
),

exclusion_data as (
    select * from desserts_data
    union all
    select * from cafe_data
),

filtered_restaurants_data as (
    select r.*
    from restaurants_data as r
    left join exclusion_data as e
    on r.id = e.id
    where e.id is null

),

yelp_all as (
    select *, 'restaurants' as type from filtered_restaurants_data
    union all
    select *, 'desserts' as type from desserts_data
    union all
    select *, 'cafe' as type from cafe_data
)

select
    yelp_all.id as id,
    yelp_all.alias as alias,
    LOWER(yelp_all.name) as name,
    yelp_all.url as url,
    yelp_all.review_count as review_count,
    yelp_all.rating as rating,
    yelp_all.price as price,
    yelp_all.categories as categories,
    LOWER(yelp_all.ethnic_category) as ethnic_categories,
    LOWER(yelp_all.type) as type,
    yelp_all.latitude as shop_latitude,
    yelp_all.longitude as shop_longitude,
    yelp_all.coordinate as shop_coordinate,
    yelp_all.address as shop_address,
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
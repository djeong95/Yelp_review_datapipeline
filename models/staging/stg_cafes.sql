{{ config(materialized="view") }}

with coffee_rawdata as (
    select *
    from {{ source('staging', 'Coffee_data_raw')  }}
    where id is not null
),

coffee_deduped as (
    SELECT *
    FROM (SELECT *,
        ROW_NUMBER() OVER(PARTITION BY id) as rn
    FROM coffee_rawdata)
    WHERE rn = 1
)

SELECT
    CAST(t.id AS STRING) AS id,
    CAST(t.alias AS STRING) AS alias,
    CAST(t.name AS STRING) AS name,
    CAST(t.url AS STRING) AS url,
    CAST(t.review_count AS INTEGER) AS review_count,
    ARRAY_TO_STRING(t.categories, ', ') as categories,
    ARRAY_TO_STRING(t. ethnic_category, ', ') as ethnic_category,
    CAST(t.rating AS NUMERIC) AS rating,
    CAST(t.price AS STRING) AS price,
    CAST(t.latitude AS NUMERIC) AS latitude,
    CAST(t.longitude AS NUMERIC) AS longitude,
    {{ get_coordinates('latitude', 'longitude') }} as coordinate,
    CAST(t.city AS STRING) AS city,
    CAST(t.address AS STRING) AS address
FROM coffee_deduped t

-- # CROSS JOIN attempt generated way too many rows; decided on making them into string separated by comma. 
-- CROSS JOIN 
--     UNNEST(t.categories) AS category
-- CROSS JOIN 
--     UNNEST(t.ethnic_category) AS ethnic


-- dbt build --m <model.sql> --vars 'is_test_run: false'

{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}

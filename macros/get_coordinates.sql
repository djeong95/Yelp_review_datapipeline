{# 
    This macro returns the string of concatenated latitude and longitude separated by a comma
#}

{% macro get_coordinates(latitude, longitude) -%}

    CAST({{latitude}} AS STRING) || ',' || CAST({{longitude}} AS STRING)

{%- endmacro %}
{% macro country_filtered_view(dataset, table, country_code) %}
    select * 
    from {{ source(dataset, table) }}
    where country_code =  '{{ country_code }}'
{% endmacro %}
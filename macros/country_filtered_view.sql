{% macro country_filtered_view(dataset, table, country_code) %}
    select * 
    from {{ source(dataset, table) }}
    where country_code =  '{{ country_code }}'
    {% if var.has_var('date') %}
        and date(refresh_date) = date('{{ var("date") }}')
    {% else %}
        {% if is_incremental() %}
            and date(refresh_date) = date_sub(current_date(), INTERVAL 1 DAY)
        {% else %}
            and date(refresh_date) < date_sub(current_date(), INTERVAL 1 DAY)
        {% endif %}
    {% endif %}
{% endmacro %}
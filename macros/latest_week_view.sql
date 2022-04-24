{% macro latest_week_view(table) %}
    select *
    from {{ ref(table) }}
    where week = (select max(week) from {{ ref(table) }})
{% endmacro %}
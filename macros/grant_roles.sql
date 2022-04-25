{% macro grant_dataset_role(dataset, user) %}}
grant `roles/bigquery.dataViewer` on schema {{ dataset }} to '{{ user }}'
{% endmacro %}}
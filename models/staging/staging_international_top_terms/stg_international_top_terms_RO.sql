
    {{ 
        config(
            schema = 'staging',
            materialized = 'incremental',
            partition_by = {
                'field': 'refresh_date',
                'data_type': 'date'
            },
            incremental_strategy = 'insert_overwrite'
        ) 
    }}
    {{ country_filtered_incremental('src_google_trends', 'international_top_terms', 'RO') }}
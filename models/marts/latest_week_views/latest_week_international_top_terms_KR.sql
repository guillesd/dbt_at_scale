
    {{ 
        config(
            schema = 'mart_latest_week',
            materialized = 'view',
        ) 
    }}
    {{ latest_week_view('stg_international_top_terms_KR') }}
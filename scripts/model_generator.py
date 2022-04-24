import csv 
from typing import Generator
import os
import logging

LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
)

def read_countries_csv() -> Generator[str, None, None]:
    with open("./scripts/inputs/countries.csv", "r") as file:
        lines = csv.reader(file)
        next(lines)
        for line in lines:
            yield line[0]

def render_staging_models(country: str, dataset: str, table: str) -> str:
    config = """
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
    """
    macro = (
        "{{ "
        f"country_filtered_incremental('{dataset}', '{table}', '{country}')" 
        " }}"
    )
    return config + macro

def render_mart_models(table: str) -> str:
    config = """
    {{ 
        config(
            schema = 'mart_latest_week',
            materialized = 'view',
        ) 
    }}
    """
    macro = (
        "{{ "
        f"latest_week_view('{table}')" 
        " }}"
    )
    return config + macro

def create_models(dataset: str, table: str) -> None:
    for country in read_countries_csv():
        logging.info(f"Creating staging model from table {table} for country {country}")
        rendered_staging = render_staging_models(country, dataset, table)

        staging_directory = f"./models/staging/staging_{table}"
        if not os.path.exists(staging_directory):
            os.makedirs(staging_directory)
        with open(f"{staging_directory}/stg_{table}_{country}.sql", "w") as out_file:
            out_file.write(rendered_staging)

        logging.info(f"Creating marts latest week view model from table {table} for country {country}")
        rendered_mart= render_mart_models(f"stg_{table}_{country}")

        marts_directory = f"./models/marts/latest_week_views"
        if not os.path.exists(marts_directory):
            os.makedirs(marts_directory)
        with open(f"{marts_directory}/latest_week_{table}_{country}.sql", "w") as out_file:
            out_file.write(rendered_mart)

if __name__=="__main__":
    for table in ["international_top_rising_terms", "international_top_terms"]:
        create_models(dataset="src_google_trends", table=table)

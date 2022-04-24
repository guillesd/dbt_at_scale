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

def render_template(country: str, dataset: str, table: str) -> str:
    config = """
    {{ 
        config(
            schema = 'staging'
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
        f"country_filtered_view('{dataset}', '{table}', '{country}')" 
        " }}"
    )
    return config + macro

def main(dataset: str, table: str) -> None:
    for country in read_countries_csv():
        logging.info(f"Creating model from table {table} for country {country}")
        rendered_template = render_template(country, dataset, table)

        directory = f"./models/staging/staging_{table}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(f"{directory}/stg_{table}_{country}.sql", "w") as out_file:
            out_file.write(rendered_template)

if __name__=="__main__":
    for table in ["international_top_rising_terms", "international_top_terms"]:
        main(dataset="src_google_trends", table=table)

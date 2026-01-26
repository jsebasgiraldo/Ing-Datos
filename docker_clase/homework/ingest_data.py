#ingestion

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
# Read a sample of the data

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='green_taxi', help='Target table name')
@click.option('--target-table2', default='zones', help='Target table name')



def run(pg_user,pg_pass, pg_host, pg_port, pg_db, target_table,target_table2):


    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    url2='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    green_taxi = pd.read_parquet(url)
    zones=pd.read_csv(url2)

    first=True
    if first:
        # Create table schema (no data)
        green_taxi.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )

        zones.head(0).to_sql(
            name=target_table2,
            con=engine,
            if_exists="replace"
        )

        first = False
        print("Tables created")


        green_taxi.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted green taxi:", len(green_taxi))

        zones.to_sql(
            name=target_table2,
            con=engine,
            if_exists="append"
        )

        print("Inserted zones:", len(zones))

if __name__=='__main__':
    run()
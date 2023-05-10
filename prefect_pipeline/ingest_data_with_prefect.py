#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import task, flow
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3)
def ingest_data(table_name, url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    database_block = SqlAlchemyConnector.load("postgres-connector")

    with database_block.get_connection(begin=False) as engine:

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

        df.to_sql(name=table_name, con=engine, if_exists="append")

        while True:
            try:
                t_start = time()

                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists="append")

                t_end = time()

                print("inserted another chunk, took %.3f second" % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f'generating logs for {table_name}')

@flow(name="Ingest Flow")
def main():
    table_name = "yellow_taxi_trips-prefect-2"
    csv_url = "http://127.0.0.1:8000/yellow_tripdata_2021-01.csv" #"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    log_subflow(table_name)
    ingest_data(table_name, csv_url)


if __name__ == "__main__":
    main()

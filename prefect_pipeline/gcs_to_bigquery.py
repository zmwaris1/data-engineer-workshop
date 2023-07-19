from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path
import pandas as pd

@task(retries=3, log_prints=True)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """Download from GCS"""
    try:
        gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
        gcs_block = GcsBucket.load("zoomcamp-gcs")
        gcs_block.get_directory(from_path=gcs_path, local_path=f"../data")
        return Path(f"../data/{gcs_path}")
    except Exception as e:
        print(None)

@task(log_prints=True)
def transform(path:Path) -> pd.DataFrame:
    """Data transformation"""
    try:
        df = pd.read_parquet(path)
        print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        df["passenger_count"].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
        return df
    except Exception as e:
        print(e)

@task(log_prints=True)
def write_to_bq(df:pd.DataFrame) -> None:
    """Write DataFrame to BigQuey"""
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="zoomcamp.rides",
        project_id="dtc-de-tutorial",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def main_flow():
    """Main ETL flow"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    print(len(df))
    write_to_bq(df)

if __name__ == "__main__":
    main_flow()
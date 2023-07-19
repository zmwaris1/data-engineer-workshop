from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os

@task(log_prints=True)
def fetch(dataset_url) -> pd.DataFrame:
    """Read data from web to pandas DataFrame."""
    print("Executing fetch")
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df['tpep_pickup_datetime']) # type: ignore
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime']) # type: ignore
    print(df.head(1)) # type: ignore
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}") # type: ignore
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write DataFrame locally as parquet file"""
    print("Executing write local")
    dir = f'data/{color}'
    if not os.path.exists(dir):
        os.makedirs(dir)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip", engine="fastparquet")
    print("Finished executing write local")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload parquet to GCS"""
    print("executing write gcs")
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path = f"{path}",
        to_path = path
    )
    print("Finished wirte gcs")
    return

@flow(log_prints=True)
def web_to_gcs() -> None:
    """The main ETL func."""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    print("flow start")
    df = fetch(dataset_url)
    df_clean = clean(df=df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    print("flow end")

if __name__ == '__main__':
    web_to_gcs()

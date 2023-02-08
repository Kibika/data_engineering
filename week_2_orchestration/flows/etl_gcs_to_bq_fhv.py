from pathlib import Path
import pandas as pd
from typing import List
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/fhv/fhv_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.fhv",
        project_id="second-chariot-375510",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(year, month)
    df = transform(path)
    write_bq(df)


@flow()
def etl_parent_flow(
    months: List[int] = [2, 3], year: int = 2019
):
    for month in months:
        etl_gcs_to_bq(year, month)


if __name__ == "__main__":
    months = [2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)
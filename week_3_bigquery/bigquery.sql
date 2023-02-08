-- Count number of rows
SELECT COUNT(1) FROM `second-chariot-375510.trips_data_all.fhv`;

-- Count distinct number of affiliated_base_number for the entire dataset in table
SELECT COUNT(DISTINCT affiliated_base_number) AS count
FROM `second-chariot-375510.trips_data_all.fhv`;

-- Count records with have both a blank (null) PUlocationID and DOlocationID in the entire dataset
SELECT COUNT(*)
FROM `second-chariot-375510.trips_data_all.fhv`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE second-chariot-375510.trips_data_all.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM second-chariot-375510.trips_data_all.fhv;

--Retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).
SELECT COUNT(DISTINCT affiliated_base_number) AS count
FROM `second-chariot-375510.trips_data_all.fhv_tripdata_partitoned_clustered`
WHERE CAST(pickup_datetime AS DATE) BETWEEN DATE("2019-03-01") AND DATE("2019-03-31");

--Retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).
SELECT COUNT(DISTINCT affiliated_base_number) AS count
FROM `second-chariot-375510.trips_data_all.fhv`
WHERE CAST(pickup_datetime AS DATE) BETWEEN DATE("2019-03-01") AND DATE("2019-03-31");

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `second-chariot-375510.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-375510-cf05a71f2b3e/data/fhv/fhv_tripdata_2019-*.parquet']
);

--Count distinct number of affiliated_base_number for the entire dataset in external table
SELECT COUNT(DISTINCT affiliated_base_number) AS count
FROM `second-chariot-375510.trips_data_all.external_fhv_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486123.rides_dataset.rides_2024_external_table`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-sebastian-giraldo-zoomcamp-2026/rides_dataset/rides/yellow_tripdata_2024_*.parquet']
);

SELECT distinct pu_location_id,count(1) FROM `kestra-sandbox-486123.rides_dataset.rides_2024_external_table`
group by pu_location_id;

SELECT distinct pu_location_id,count(1) FROM `kestra-sandbox-486123.rides_dataset.rides`
group by pu_location_id;

SELECT pu_location_id FROM `kestra-sandbox-486123.rides_dataset.rides`;
SELECT pu_location_id,do_location_id FROM `kestra-sandbox-486123.rides_dataset.rides`;

SELECT COUNT(1) FROM `kestra-sandbox-486123.rides_dataset.rides_2024_external_table`
WHERE fare_amount =0;

CREATE OR REPLACE TABLE `kestra-sandbox-486123.rides_dataset.rides_2024_partition`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY vendor_id
AS SELECT * FROM `kestra-sandbox-486123.rides_dataset.rides_2024_external_table`;

SELECT DISTINCT vendor_id FROM `kestra-sandbox-486123.rides_dataset.rides`
WHERE tpep_dropoff_datetime between "2024-03-01" and "2024-03-15";

SELECT DISTINCT vendor_id FROM `kestra-sandbox-486123.rides_dataset.rides_2024_partition`
WHERE tpep_dropoff_datetime between "2024-03-01" and "2024-03-15";

SELECT count(*) FROM `kestra-sandbox-486123.rides_dataset.rides`;
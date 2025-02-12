--set up
create or replace external table `de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi`
OPTIONS (
  format = 'parquet',
  uris = ['gs://huyde_dezoomcamp_hw3_2025/yellow_tripdata_2024-*.parquet']
);

--question 1
select count(1) from de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi
-- 20332093
;

--question 2
select distinct PULocationID from de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi
-- This query will process 155.12 MB when run.
;

select distinct PULocationID from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi
--This query will process 155.12 MB when run.
;

--question 3
select PULocationID from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi
--Bytes processed
--155.12 MB
;

select PULocationID, DOLocationID  from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi
--Bytes processed
--310.24 MB

;

--question 4
select count(1) from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi where fare_amount = 0
--8333
;

--question 5
create table de-zoomcamp-2025.nytaxi_dataset.yellow_taxi_update 
partition by DATE(tpep_dropoff_datetime)
CLUSTER by VendorID
as select * from de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi
;

--question 6
select distinct vendorid from  de-zoomcamp-2025.nytaxi_dataset.yellow_taxi
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'
--This query will process 310.24 MB when run.

;
select distinct vendorid from  de-zoomcamp-2025.nytaxi_dataset.yellow_taxi_update
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'
--This query will process 26.84 MB when run
;

delete from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi where vendorId = 2 and trip_distance = 2.62 and fare_amount = 19.8;
select * from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi
;
delete from de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi where vendorId = 2 and trip_distance = 2.62 and fare_amount = 19.8;
;

--question 9
select count(*) from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi_update;
-- 20332093
select count(*) from de-zoomcamp-2025.nytaxi_dataset.external_yellow_taxi;
--20332093
select count(*) from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi;
-- 20269694
-- 20268761
select count(*) from de-zoomcamp-2025.nytaxi_dataset.yellow_taxi where vendorid = 1;

/*
  Bigquery tell 0 bytes will be estimated when I run this query. Because, I have ran this query before (at quetion 2)
  , Bigquery save excute plan for this query. So, it do not need to estimate any byte for this query. 
  Just take result from history
*/

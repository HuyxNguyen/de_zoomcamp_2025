{{
    config(
        materialized='table'
    )
}}

with green_trip_data as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_trip_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
trips_unioned as (
    select 
        'green' as type_taxi, * 
    from green_trip_data
    union all 
    select 
        'yellow' as type_taxi, * 
    from yellow_trip_data
)
select
    type_taxi
    , EXTRACT(YEAR from pickup_datetime) || '/Q' || EXTRACT(QUARTER from pickup_datetime) as Quater_Time
    , sum(total_amount) as Quarterly_Revenues
from trips_unioned
group by EXTRACT(YEAR from pickup_datetime) || '/Q' || EXTRACT(QUARTER from pickup_datetime), type_taxi
--homework/de_zoomcamp_2025/w4_dbt/solution_homework/models/marts/fct_taxi_trips_quarterly_revenue.sql
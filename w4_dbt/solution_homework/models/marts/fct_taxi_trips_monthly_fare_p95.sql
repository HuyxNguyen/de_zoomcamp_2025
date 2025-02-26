with green_fact_trips as (
select
    service_type
    , pickup_month
    , pickup_year
    , percentile_cont(fare_amount, 0.90) over (partition by service_type, pickup_year, pickup_month) as p90_fare
    , percentile_cont(fare_amount, 0.95) over (partition by service_type, pickup_year, pickup_month) as p95_fare
    , percentile_cont(fare_amount, 0.97) over (partition by service_type, pickup_year, pickup_month) as p97_fare
from {{ref('fact_trips')}}
where 
    pickup_month = 4 and pickup_year = 2020
    and service_type = 'Green'
    and fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit Card')
limit 1
),
yellow_fact_trips as (
  select
      service_type
      , pickup_month
      , pickup_year
      , percentile_cont(fare_amount, 0.90) over (partition by service_type, pickup_year, pickup_month) as p90_fare
      , percentile_cont(fare_amount, 0.95) over (partition by service_type, pickup_year, pickup_month) as p95_fare
      , percentile_cont(fare_amount, 0.97) over (partition by service_type, pickup_year, pickup_month) as p97_fare
  from {{ref('fact_trips')}}
  where 
      pickup_month = 4 and pickup_year = 2020
      and service_type = 'Yellow'
      and fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
  limit 1
)
select * from green_fact_trips
union all 
select * from yellow_fact_trips

-- question 5 --

--green--
select
  a.Quater_Time
  , a.Quarterly_Revenues
  , b.Quater_Time
  , b.Quarterly_Revenues
  , (a.Quarterly_Revenues - b.Quarterly_Revenues) / b.Quarterly_Revenues as rev_growth
from `nytaxi_dataset.fct_taxi_trips_quarterly_revenue` a
left join `nytaxi_dataset.fct_taxi_trips_quarterly_revenue` b
on cast(LEFT(a.Quater_Time, 4) as int) = cast(LEFT(b.Quater_Time, 4) as int) + 1
  and RIGHT(a.Quater_Time, 2) = right(b.Quater_Time, 2)
  and a.type_taxi = b.type_taxi
where 
    a.type_taxi = 'green'    
    and cast(LEFT(a.Quater_Time, 4) as int) = 2020
;

--yellow--
select
  a.Quater_Time
  , a.Quarterly_Revenues
  , b.Quater_Time
  , b.Quarterly_Revenues
  , (a.Quarterly_Revenues - b.Quarterly_Revenues) / b.Quarterly_Revenues as rev_growth
from `nytaxi_dataset.fct_taxi_trips_quarterly_revenue` a
left join `nytaxi_dataset.fct_taxi_trips_quarterly_revenue` b
on cast(LEFT(a.Quater_Time, 4) as int) = cast(LEFT(b.Quater_Time, 4) as int) + 1
  and RIGHT(a.Quater_Time, 2) = right(b.Quater_Time, 2)
  and a.type_taxi = b.type_taxi
where 
    a.type_taxi = 'yellow'    
    and cast(LEFT(a.Quater_Time, 4) as int) = 2020
;
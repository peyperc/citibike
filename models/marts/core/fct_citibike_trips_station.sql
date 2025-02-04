with data as (
    select * from {{ref("int_citibike_weather")}}
)


select to_char(START_TIME::date,'yyyymmdd') date_key,
start_station_id station_key,
count(1) trip_count
from  data
group by START_TIME::date,start_station_id

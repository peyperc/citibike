with data as (
    select * from {{ref("int_citibike_weather")}}
)


select to_char(START_TIME::date,'yyyymmdd') date_key,
(case when weather_id = 'unknown' then '0' else weather_id end)::int as weather_key,
start_station_id as station_key,
count(1) trip_count, 
sum(haversine(start_station_latitude,start_station_longitude,end_station_latitude,end_station_longitude) * 100)::int trip_distance,
sum(trip_duration) trip_duration,
count(distinct bike_id) bike_count,
count(distinct weather_id) weather_count
from  data
group by START_TIME::date,weather_id,station_key

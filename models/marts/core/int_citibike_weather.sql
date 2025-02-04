with citibike as (
    select * from {{ref("stg_citibike_trips")}}
),

weather as (
    select * from {{ref("stg_weather_nyc")}}
)

SELECT c.trip_duration ,
c.start_time ,
c.stop_time ,
c.start_station_id ,
c.start_station_name ,
c.start_station_latitude ,
c.start_station_longitude ,
c.end_station_id ,
c.end_station_name ,
c.end_station_latitude ,
c.end_station_longitude ,
c.bike_id ,
c.user_type ,
c.birth_year::int as birth_year ,
year(current_date()) - c.birth_year::int as Age,
c.gender,
coalesce(w.weather,'unknown')as weather,
coalesce(w.weather_id,'unknown')as weather_id
from DATAMART.DBT_CPEYPER.stg_citibike_trips c
left join datamart.dbt_cpeyper.weather_nyc w on c.start_time >= w.start_time and c.start_time < w.end_time 
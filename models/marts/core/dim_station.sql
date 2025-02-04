
with data as (
    select * from {{ref("int_citibike_weather")}}
),

 station as (
Select distinct start_station_id::int as station_key,
start_station_name as station_name,
start_station_latitude as station_latitude,
start_station_longitude as station_longitude
FROM data

union

Select distinct end_station_id::int as station_key,
end_station_name as station_name,
end_station_latitude as station_latitude,
end_station_longitude as station_longitude
FROM data)


Select *
from station
qualify row_number() over (partition by station_key order by length(station_latitude) desc,station_latitude asc ) = 1
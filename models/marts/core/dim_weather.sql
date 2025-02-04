with data as (
    select * from {{ref("int_citibike_weather")}}
)

Select distinct weather as weather_type, 
(case when weather_id = 'unknown' then '0' else weather_id end)::int as weather_key
FROM data
order by 2 asc
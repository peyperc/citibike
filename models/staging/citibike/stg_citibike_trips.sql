with
    source as (select * from {{ source("citibike", "citibike_trips") }}),

    renamed as (
        select
            trip_duration::int as trip_duration,
            to_timestamp(start_time, 'MM/DD/YYYY HH24:MI') as start_time,
            to_timestamp(stop_time, 'MM/DD/YYYY HH24:MI') as stop_time,
            start_station_id as start_station_id,
            start_station_name,
            start_station_latitude::float as start_station_latitude,
            start_station_longitude::float as start_station_longitude,
            coalesce(
                end_station_id,
                lead(start_station_id) over (partition by bikeid order by start_time)
            ) as end_station_id,
            coalesce(
                end_station_name,
                lead(start_station_name) over (partition by bikeid order by start_time)
            ) as end_station_name,
            coalesce(
                end_station_latitude,
                lead(start_station_latitude) over (
                    partition by bikeid order by start_time
                )
            )::float as end_station_latitude,
            coalesce(
                end_station_longitude,
                lead(start_station_longitude) over (
                    partition by bikeid order by start_time
                )
            )::float as end_station_longitude,
            bikeid::int as bike_id,
            usertype as user_type,
            COALESCE(birth_year, to_char(YEAR(current_date()))) as birth_year,
            gender::int as gender
        from source
        where
            start_time regexp (
                '[0-9]{0,2}\/[0-9]{0,2}\/[0-9][0-9][0-9][0-9]\ [0-9]{0,2}\:[0-9]{0,2}'
            )
        union
        select
            trip_duration::int as trip_duration,
            start_time::timestamp as start_time,
            stop_time::timestamp as stop_time,
            start_station_id as start_station_id,
            start_station_name,
            start_station_latitude::float as start_station_latitude,
            start_station_longitude::float as start_station_longitude,
            coalesce(
                end_station_id,
                lead(start_station_id) over (partition by bikeid order by start_time)
            ) as end_station_id,
            coalesce(
                end_station_name,
                lead(start_station_name) over (partition by bikeid order by start_time)
            ) as end_station_name,
            coalesce(
                end_station_latitude,
                lead(start_station_latitude) over (
                    partition by bikeid order by start_time
                )
            )::float as end_station_latitude,
            coalesce(
                end_station_longitude,
                lead(start_station_longitude) over (
                    partition by bikeid order by start_time
                )
            )::float as end_station_longitude,
            bikeid::int as bike_id,
            usertype as user_type,
            COALESCE(birth_year, to_char(YEAR(current_date()))) as birth_year,
            gender::int as gender
        from source
        where
            start_time not regexp (
                '[0-9]{0,2}\/[0-9]{0,2}\/[0-9][0-9][0-9][0-9]\ [0-9]{0,2}\:[0-9]{0,2}'
            )
    )

select *
from renamed

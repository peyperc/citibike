with
    source as (select * from {{ source("weatherdata", "weatherdata_ny") }}),

    renamed as (
        select
            to_timestamp(data:time::int) as start_time,
            lead(start_time) over (partition by DATA:city:id::int order by start_time) as end_time,
            data:city:id::int as city_id,
            data:city:country::string as country,
            data:city:name::string as city,
            data:weather[0]:main::string as weather,
            data:weather[0]:id::string as weather_id,
            data:main:humidity::int as humidity,
            data:main:pressure::int as pressure,
            data:main:temp::number(8, 2) as temp,
            data:main:temp_max::number(8, 2) as temp_max,
            data:main:temp_min::number(8, 2) as temp_min
        from source
        where data:city:id::int = 5128581
    )

select *
from renamed

with
    source as (select * from {{ source("weatherdata", "weatherdata_ny") }}),

    renamed as (
        select
            to_timestamp(data:time::int) as readingtime,
            data:weather[0]:main::string as weather,
            data:weather[0]:id::string as id,
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

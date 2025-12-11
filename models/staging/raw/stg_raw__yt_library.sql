with source as (

    select *
    from {{ source('raw', 'yt_library') }}

),

dedup as (

    select
        *,
        row_number() over (
            partition by track_id
            order by extraction_date desc   -- garde la plus récente uniquement
        ) as rn
    from source

),

renamed as (

    select
        track_id,
        title,
        artist,
        album,
        duration_seconds,
        liked,
        ytm_url,
        source,
        extraction_date
    from dedup
    where rn = 1  -- on garde une seule ligne par track_id

)

select *
from renamed

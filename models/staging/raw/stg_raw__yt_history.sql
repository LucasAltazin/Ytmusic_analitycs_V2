with 

source as (

    select * from {{ source('raw', 'yt_history') }}

),

renamed as (

    select
        concat(track_id, '_', cast(played_at as string)) as listening_id,
        track_id,
        title,
        artist,
        album,
        duration_seconds,
        liked,
        ytm_url,
        source.source AS source,
        played_at,
        extraction_date

    from source 

)

select * from renamed 
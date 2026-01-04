with 

source as (

    select * from {{ source('raw', 'yt_history') }}

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
        played_at,
        extraction_date

    from source

)

select * from renamed
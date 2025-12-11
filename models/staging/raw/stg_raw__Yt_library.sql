with 

source as (

    select * from {{ source('raw', 'Yt_library') }}

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

    from source

)

select * from renamed
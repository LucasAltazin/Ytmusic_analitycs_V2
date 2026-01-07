with 

source as (

    select * from {{ source('raw', 'spotify_history') }}

),

renamed as (

    select
        source_track_id,
        title_original,
        artist_original,
        album_original,
        source,
        spotify_track_id,
        spotify_artist_id,
        spotify_album_id,
        release_year,
        duration_ms,
        duration_seconds,
        popularity,
        explicit,
        genres,
        extraction_date

    from source

)

select * from renamed
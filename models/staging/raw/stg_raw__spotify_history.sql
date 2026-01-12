with 

source as (

    select * from {{ source('raw', 'spotify_history') }}

),

renamed as (

    select
        concat(source_track_id, '_', cast(source_played_at as string)) as listening_id,
        source_track_id,
        source_played_at,
        title_original,
        artist_original,
        album_original,
        source.source AS source,
        spotify_track_id,
        spotify_artist_id,
        spotify_album_id,
        release_year,
        duration_ms,
        duration_seconds,
        popularity,
        explicit,
        trim(split(genres, ',')[SAFE_OFFSET(0)]) as genres,
        extraction_date

    from source

)

select * from renamed
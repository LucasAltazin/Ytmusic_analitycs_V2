with source as (

    select * 
    from {{ source('raw', 'spotify_library') }}

),

dedup as (

    select
        *,
        row_number() over (
            partition by source_track_id
            order by 
                extraction_date desc,   -- garde la plus récente
                popularity desc         -- puis la plus populaire
        ) as rn
    from source

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
    from dedup
    where rn = 1   -- on garde une seule ligne par source_track_id

)

select * 
from renamed

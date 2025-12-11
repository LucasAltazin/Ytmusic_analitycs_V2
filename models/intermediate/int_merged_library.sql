with spotify as (

    select
        source_track_id,
        spotify_track_id,
        title_original,
        artist_original,
        album_original,
        source,
        release_year,
        duration_ms,
        duration_seconds,
        popularity,
        genres,
        extraction_date
    from {{ ref('stg_raw__spotify_library') }}

),

yt as (

    select
        track_id,
        ytm_url
    from {{ ref('stg_raw__yt_library') }}

),

genre as (

    select
        spotify_raw_genre,
        main_genre,
        sub_genre
    from {{ ref('stg_raw__genre') }}

),

joined as (

    select
        s.source_track_id        as yt_track_id,
        s.spotify_track_id,
        s.title_original         as title,
        s.artist_original        as artist,
        s.album_original         as album,
        s.source,
        s.release_year,
        s.duration_ms,
        s.duration_seconds,
        s.popularity,
        s.genres,
        s.extraction_date,
        y.ytm_url,
        g.main_genre,
        g.sub_genre
    from spotify s
    left join yt y
        on y.track_id = s.source_track_id
    left join genre g
        on g.spotify_raw_genre = s.genres
)

select *
from joined

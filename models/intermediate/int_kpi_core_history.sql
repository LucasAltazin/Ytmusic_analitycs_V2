with core as (

    select
        listening_id,

        -- cast STRING → TIMESTAMP (required by schema)
        timestamp(played_at) as played_at_ts,

        artist,
        title,
        spotify_track_id,
        duration_seconds,
        main_genre,
        sub_genre

    from {{ ref('int_merged_history') }}

)

select
    listening_id,

    -- add the raw timestamp to the output
    played_at_ts,

    -- time dimensions
    date(played_at_ts)                               as listen_date,
    extract(hour from played_at_ts)                  as listen_hour,
    extract(dayofweek from played_at_ts)            as listen_weekday,       -- 1 (Sun) → 7 (Sat)
    format_timestamp('%A', played_at_ts)            as listen_weekday_name,

    -- identity
    artist,
    title,
    spotify_track_id,

    -- metrics
    duration_seconds,
    duration_seconds / 60.0                          as duration_minutes,
    duration_seconds / 3600.0                        as duration_hours,

    -- dimensions
    main_genre,
    sub_genre,

    -- quality flags
    played_at_ts is not null                         as has_played_at,
    main_genre is not null                           as has_genre

from core

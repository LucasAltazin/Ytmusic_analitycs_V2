-- models/intermediate/int_kpi_core_history.sql

with core as (

    select
        listening_id,  -- keep PK for traceability

        -- cast STRING → TIMESTAMP and filter nulls
        timestamp(played_at) as played_at_ts

        , artist
        , title
        , spotify_track_id
        , duration_seconds
        , main_genre
        , sub_genre

    from {{ ref('int_merged_history') }}
    where played_at is not null  -- filter nulls here

)

select
    listening_id,

    -- raw timestamp (keep for debugging / trace)
    played_at_ts,

    -- time dimensions
    date(played_at_ts)                               as listen_date,
    extract(hour from played_at_ts)                  as listen_hour,
    extract(dayofweek from played_at_ts)            as listen_weekday,
    format_timestamp('%A', played_at_ts)            as listen_weekday_name,

    -- identity
    artist,
    title,
    spotify_track_id,
    main_genre,
    sub_genre,

    -- metrics
    duration_seconds,
    duration_seconds / 60.0                          as duration_minutes,
    duration_seconds / 3600.0                        as duration_hours,

    -- quality flags
    played_at_ts is not null                         as has_played_at,
    main_genre is not null                           as has_genre,

    -- recalculated listen_count per track per day
    count(*) over (partition by date(played_at_ts), spotify_track_id) as listen_count

from core
-- KPIs aggregated at TRACK level
-- Grain: 1 row per track

select
    spotify_track_id,
    title,
    artist,
    main_genre,

    -- Core usage
    sum(listen_count)                            as total_listens,
    count(distinct listen_date)                  as active_days,

    -- Time
    sum(duration_minutes)                        as total_minutes,
    sum(duration_hours)                          as total_hours,

    -- Replay vs discovery proxy
    safe_divide(
        sum(listen_count),
        count(distinct listen_date)
    )                                            as avg_listens_per_day

from {{ ref('int_kpi_core_history') }}

where spotify_track_id is not null

group by
    spotify_track_id,
    title,
    artist,
    main_genre

order by total_listens desc
